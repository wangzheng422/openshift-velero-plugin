package main

import (
	veleroplugin "github.com/vmware-tanzu/velero/pkg/plugin/framework"
)

func main() {
	veleroplugin.NewServer().
		RegisterBackupItemAction("wzhlab.top/create-pvc-from-snapshot-action", NewCreatePvcFromSnapshotAction).
		Serve()
}
