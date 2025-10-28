package main

import (
	veleroplugin "github.com/vmware-tanzu/velero/pkg/plugin/framework"
)

func main() {
	veleroplugin.NewServer().
		RegisterRestoreItemAction("wzhlab.top/modify-csi-volume-handle-action", NewModifyVolumeHandleAction).
		Serve()
}
