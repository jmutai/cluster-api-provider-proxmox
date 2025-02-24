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
	bootDvice = "scsi0"
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

		// no qemu found, try to create new one
		log.V(3).Info("qemu wasn't found. new qemu will be created")
		if exist, err := s.client.VirtualMachineExistsWithName(ctx, s.scope.Name()); exist || err != nil {
			if exist {
				// there should no qemu with same name. occuring an error
				err = fmt.Errorf("qemu %s already exists", s.scope.Name())
			}
			log.Error(err, "stop creating new qemu to avoid replicating same qemu")
			return nil, err
		}
		qemu, err = s.createQEMU(ctx)
		if err != nil {
			log.Error(err, "failed to create qemu")
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

// get QEMU gets proxmox vm from vmid
func (s *Service) getQEMU(ctx context.Context) (*proxmox.VirtualMachine, error) {
	log := log.FromContext(ctx)
	log.Info("getting qemu from vmid")
	vmid := s.scope.GetVMID()
	if vmid != nil {
		return s.client.VirtualMachine(ctx, *vmid)
	}
	return nil, rest.NotFoundErr
}

func (s *Service) createQEMU(ctx context.Context) (*proxmox.VirtualMachine, error) {
	log := log.FromContext(ctx)
	log.Info("creating qemu")

	// create qemu
	log.Info("making qemu spec")
	vmoption := s.generateVMOptions()
	// bind annotation key-values to context
	schedCtx := framework.ContextWithMap(ctx, s.scope.Annotations())
	result, err := s.scheduler.CreateQEMU(schedCtx, &vmoption)
	if err != nil {
		log.Error(err, "failed to schedule qemu instance")
		return nil, err
	}
	node, vmid, storage := result.Node(), result.VMID(), result.Storage()
	s.scope.SetNodeName(node)
	s.scope.SetVMID(vmid)

	// inject storage
	s.injectVMOption(&vmoption, storage)
	s.scope.SetStorage(storage)

	// os image
	if err := s.setCloudImage(ctx); err != nil {
		return nil, err
	}

	// actually create qemu
	vm, err := s.client.CreateVirtualMachine(ctx, node, vmid, vmoption)
	if err != nil {
		return nil, err
	}

	return vm, nil
}

func (s *Service) generateVMOptions() api.VirtualMachineCreateOptions {
	vmName := s.scope.Name()
	snippetStorageName := s.scope.GetClusterStorage().Name
	imageStorageName := s.scope.GetStorage()
	network := s.scope.GetNetwork()
	hardware := s.scope.GetHardware()
	options := s.scope.GetOptions()
	cicustom := fmt.Sprintf("user=%s:%s", snippetStorageName, userSnippetPath(vmName))
	ide2 := fmt.Sprintf("file=%s:cloudinit,media=cdrom", imageStorageName)
	// scsi0 := fmt.Sprintf("%s:0,import-from=%s", imageStorageName, rawImageFilePath(s.scope.GetImage()))
	net0 := hardware.NetworkDevice.String()
	// Assign primary SCSI disk
	scsiDisks := api.Scsi{
		Scsi0: fmt.Sprintf("%s:0,import-from=%s", imageStorageName, rawImageFilePath(s.scope.GetImage())),
	}
	// Assign additional disks manually
	extraDisks := s.scope.GetHardware().ExtraDisks
	if len(extraDisks) > 5 {
		log.FromContext(context.TODO()).Error(fmt.Errorf("too many extra disks"), "Only 6 extra disks are supported, ignoring extra disks")
		extraDisks = extraDisks[:6] // Trim to 5 disks
	}

	for i, disk := range extraDisks {
		switch i {
		case 0:
			scsiDisks.Scsi1 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
		case 1:
			scsiDisks.Scsi2 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
		case 2:
			scsiDisks.Scsi3 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
		case 3:
			scsiDisks.Scsi4 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
		case 4:
			scsiDisks.Scsi5 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
		case 5:
			scsiDisks.Scsi6 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
		}
	}
	vmoptions := api.VirtualMachineCreateOptions{
		ACPI:          boolToInt8(options.ACPI),
		Agent:         "enabled=1",
		Arch:          api.Arch(options.Arch),
		Balloon:       options.Balloon,
		BIOS:          string(hardware.BIOS),
		Boot:          fmt.Sprintf("order=%s", bootDvice),
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

func boolToInt8(b bool) int8 {
	if b {
		return 1
	}
	return 0
}

func (s *Service) injectVMOption(vmOption *api.VirtualMachineCreateOptions, storage string) *api.VirtualMachineCreateOptions {
	// storage is finalized after node scheduling so we need to inject storage name here
	ide2 := fmt.Sprintf("file=%s:cloudinit,media=cdrom", storage)
	vmOption.Ide.Ide2 = ide2
	vmOption.Storage = storage

	// scsi0 := fmt.Sprintf("%s:0,import-from=%s", storage, rawImageFilePath(s.scope.GetImage()))
	// vmOption.Scsi.Scsi0 = scsi0
	// Assign primary root disk
	vmOption.Scsi.Scsi0 = fmt.Sprintf("%s:0,import-from=%s", storage, rawImageFilePath(s.scope.GetImage()))

	// Assign Extra Disks
	extraDisks := s.scope.GetHardware().ExtraDisks
	if len(extraDisks) > 0 {
		if len(extraDisks) > 5 {
			return nil // Returning nil indicates an error
		}
		for i, disk := range extraDisks {
			switch i {
			case 0:
				vmOption.Scsi.Scsi1 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
			case 1:
				vmOption.Scsi.Scsi2 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
			case 2:
				vmOption.Scsi.Scsi3 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
			case 3:
				vmOption.Scsi.Scsi4 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
			case 4:
				vmOption.Scsi.Scsi5 = fmt.Sprintf("%s:%d,%s", disk.Storage, i+1, disk.Size)
			}
		}
	}

	return vmOption
}
