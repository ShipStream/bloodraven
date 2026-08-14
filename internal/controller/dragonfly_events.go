package controller

// Event reasons emitted by the Dragonfly subsystem. Stable strings —
// downstream filtering depends on them. See site/content/docs/8.observability/7.log-schema.md
// for the full reference.
const (
	ReasonDragonflyPromotionStarted         = "DragonflyPromotionStarted"
	ReasonDragonflyPromotionCompleted       = "DragonflyPromotionCompleted"
	ReasonDragonflyPromotionFailed          = "DragonflyPromotionFailed"
	ReasonDragonflyStaleMasterDetected      = "DragonflyStaleMasterDetected"
	ReasonDragonflySyncTimeout              = "DragonflySyncTimeout"
	ReasonDragonflyUpgradeStarted           = "DragonflyUpgradeStarted"
	ReasonDragonflyUpgradeRejected          = "DragonflyUpgradeRejected"
	ReasonDragonflyUpgradeSnapshotStarted   = "DragonflyUpgradeSnapshotStarted"
	ReasonDragonflyUpgradeSnapshotCompleted = "DragonflyUpgradeSnapshotCompleted"
	ReasonDragonflyUpgradeCompleted         = "DragonflyUpgradeCompleted"
	ReasonDragonflyUpgradeFailed            = "DragonflyUpgradeFailed"
	// ReasonDragonflyOldSiteReconfigured fires when DragonflyManager
	// auto-attaches a stale-master pod as a replica of the active
	// master after verifying it provably never accepted writes
	// (connected_slaves=0 && master_repl_offset=0).
	ReasonDragonflyOldSiteReconfigured = "DragonflyOldSiteReconfigured"
)
