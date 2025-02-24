package instance

import (
	"context"
	"fmt"

	"github.com/k8s-proxmox/cluster-api-provider-proxmox/cloud/scheduler/framework"
	"github.com/k8s-proxmox/proxmox-go/api"
	"github.com/k8s-proxmox/proxmox-go/proxmox"
	"github.com/k8s-proxmox/proxmox-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	bootDevice   = "scsi0"
	maxScsiDisks = 6 // Proxmox SCSI disk limit
)

// reconciles QEMU instance
func (s *Service) reconcileQEMU(ctx context.Context) (*proxmox.VirtualMachine, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling QEMU")

	qemu, err := s.getQEMU(ctx)
	if err != nil {
		if !rest.IsNotFound(err) {
			log.Error(err, "failed to get qemu")
			return nil, err
		}

		// No QEMU found, create new one
		log.V(3).Info("QEMU wasn't found. Creating new QEMU...")
		if exist, err := s.client.VirtualMachineExistsWithName(ctx, s.scope.Name()); exist || err != nil {
			if exist {
				err = fmt.Errorf("QEMU %s already exists", s.scope.Name())
			}
			log.Error(err, "Stopping QEMU creation to avoid duplicate instances")
			return nil, err
		}
		qemu, err = s.createQEMU(ctx)
		if err != nil {
			log.Error(err, "Failed to create QEMU")
			return nil, err
		}
	}

	s.scope.SetVMID(qemu.VM.VMID)
	s.scope.SetNodeName(qemu.Node)
	if err := s.scope.PatchObject(); err != nil {
		return nil, err
	}
	return qemu, nil
}

// Get QEMU instance from VM ID
func (s *Service) getQEMU(ctx context.Context) (*proxmox.VirtualMachine, error) {
	log := log.FromContext(ctx)
	log.Info("Fetching QEMU from VM ID")
	vmid := s.scope.GetVMID()
	if vmid != nil {
		return s.client.VirtualMachine(ctx, *vmid)
	}
	return nil, rest.NotFoundErr
}

// Create QEMU instance
func (s *Service) createQEMU(ctx context.Context) (*proxmox.VirtualMachine, error) {
	log := log.FromContext(ctx)
	log.Info("Creating QEMU instance")

	// Generate QEMU options
	vmoption := s.generateVMOptions()
	schedCtx := framework.ContextWithMap(ctx, s.scope.Annotations())
	result, err := s.scheduler.CreateQEMU(schedCtx, &vmoption)
	if err != nil {
		log.Error(err, "Failed to schedule QEMU instance")
		return nil, err
	}

	node, vmid, storage := result.Node(), result.VMID(), result.Storage()
	s.scope.SetNodeName(node)
	s.scope.SetVMID(vmid)

	// Inject final storage
	s.injectVMOption(&vmoption, storage)
	s.scope.SetStorage(storage)

	// Set OS image
	if err := s.setCloudImage(ctx); err != nil {
		return nil, err
	}

	// Create QEMU instance
	vm, err := s.client.CreateVirtualMachine(ctx, node, vmid, vmoption)
	if err != nil {
		return nil, err
	}

	return vm, nil
}

// Generate VM options
func (s *Service) generateVMOptions() api.VirtualMachineCreateOptions {
	vmName := s.scope.Name()
	snippetStorageName := s.scope.GetClusterStorage().Name
	imageStorageName := s.scope.GetStorage()
	network := s.scope.GetNetwork()
	hardware := s.scope.GetHardware()
	options := s.scope.GetOptions()

	cicustom := fmt.Sprintf("user=%s:%s", snippetStorageName, userSnippetPath(vmName))
	ide2 := fmt.Sprintf("file=%s:cloudinit,media=cdrom", imageStorageName)
	net0 := hardware.NetworkDevice.String()

	// Assign root SCSI disk
	scsiDisks := api.Scsi{}
	scsiDisks.Scsi0 = fmt.Sprintf("%s:0,import-from=%s", imageStorageName, rawImageFilePath(s.scope.GetImage()))

	// Assign additional disks
	extraDisks := s.scope.GetHardware().ExtraDisks
	if len(extraDisks) > maxScsiDisks {
		log.FromContext(context.TODO()).Error(fmt.Errorf("too many extra disks"), "Only 6 SCSI disks are supported, ignoring extra disks")
		extraDisks = extraDisks[:maxScsiDisks] // Limit to 6 disks
	}

	for i, disk := range extraDisks {
		slot := i + 1 // scsi1, scsi2, scsi3...
		scsiDisks.Set(fmt.Sprintf("Scsi%d", slot), fmt.Sprintf("%s:%d,size=%s", disk.Storage, slot, disk.Size))
	}

	// Define VM options
	vmoptions := api.VirtualMachineCreateOptions{
		ACPI:          boolToInt8(options.ACPI),
		Agent:         "enabled=1",
		Arch:          api.Arch(options.Arch),
		Balloon:       options.Balloon,
		BIOS:          string(hardware.BIOS),
		Boot:          fmt.Sprintf("order=%s", bootDevice),
		CiCustom:      cicustom,
		Cores:         hardware.CPU,
		Cpu:           hardware.CPUType,
		CpuLimit:      hardware.CPULimit,
		Description:   options.Description,
		HugePages:     options.HugePages.String(),
		Ide:           api.Ide{Ide2: ide2},
		IPConfig:      api.IPConfig{IPConfig0: network.IPConfig.String()},
		KeepHugePages: boolToInt8(options.KeepHugePages),
		KVM:           boolToInt8(options.KVM),
		LocalTime:     boolToInt8(options.LocalTime),
		Lock:          string(options.Lock),
		Memory:        hardware.Memory,
		Name:          vmName,
		NameServer:    network.NameServer,
		Net:           api.Net{Net0: net0},
		Numa:          boolToInt8(options.NUMA),
		Node:          s.scope.NodeName(),
		OnBoot:        boolToInt8(options.OnBoot),
		OSType:        api.OSType(options.OSType),
		Protection:    boolToInt8(options.Protection),
		Reboot:        int(boolToInt8(options.Reboot)),
		Scsi:          scsiDisks,
		ScsiHw:        api.VirtioScsiPci,
		SearchDomain:  network.SearchDomain,
		Serial:        api.Serial{Serial0: "socket"},
		Shares:        options.Shares,
		Sockets:       hardware.Sockets,
		Tablet:        boolToInt8(options.Tablet),
		Tags:          options.Tags.String(),
		TDF:           boolToInt8(options.TimeDriftFix),
		Template:      boolToInt8(options.Template),
		VCPUs:         options.VCPUs,
		VMGenID:       options.VMGenerationID,
		VMID:          s.scope.GetVMID(),
		VGA:           "serial0",
	}
	return vmoptions
}

// Convert bool to int8
func boolToInt8(b bool) int8 {
	if b {
		return 1
	}
	return 0
}

// Inject final storage into VM options
func (s *Service) injectVMOption(vmOption *api.VirtualMachineCreateOptions, storage string) *api.VirtualMachineCreateOptions {
	ide2 := fmt.Sprintf("file=%s:cloudinit,media=cdrom", storage)
	vmOption.Ide.Ide2 = ide2
	vmOption.Storage = storage

	// Assign primary root disk
	vmOption.Scsi.Scsi0 = fmt.Sprintf("%s:0,import-from=%s", storage, rawImageFilePath(s.scope.GetImage()))

	// Assign extra disks
	extraDisks := s.scope.GetHardware().ExtraDisks
	for i, disk := range extraDisks {
		slot := i + 1
		vmOption.Scsi.Set(fmt.Sprintf("Scsi%d", slot), fmt.Sprintf("%s:%d,size=%s", disk.Storage, slot, disk.Size))
	}

	return vmOption
}
