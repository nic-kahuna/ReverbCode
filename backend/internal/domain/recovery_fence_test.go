package domain

import "testing"

func TestRecoveryFenceStatusAllowsNormalBootOnlyForExactSupportedInactive(t *testing.T) {
	protocol := RecoveryFenceProtocolVersion
	generation := int64(7)
	exact := RecoveryFenceStatus{
		SupportedProtocolVersion:       RecoveryFenceProtocolVersion,
		SupportedDatabaseSchemaVersion: 25,
		DatabaseSchemaVersion:          25,
		ProtocolVersion:                &protocol,
		State:                          RecoveryFenceStateInactive,
		Disposition:                    RecoveryFenceDispositionInactive,
		ReasonCode:                     RecoveryFenceReasonSupportedInactive,
		RowCount:                       1,
		ProtocolStorageClass:           "integer",
		StateStorageClass:              "text",
		PayloadStorageClass:            "blob",
		PayloadByteLength:              len(RecoveryFenceCanonicalPayload),
		PayloadSHA256:                  RecoveryFenceCanonicalPayloadSHA256,
		Generation:                     &generation,
	}
	if !exact.AllowsNormalBoot() {
		t.Fatal("exact supported inactive fence did not authorize normal boot")
	}

	tests := map[string]func(*RecoveryFenceStatus){
		"active disposition": func(s *RecoveryFenceStatus) { s.Disposition = RecoveryFenceDispositionActive },
		"future protocol":    func(s *RecoveryFenceStatus) { future := int64(2); s.ProtocolVersion = &future },
		"active state":       func(s *RecoveryFenceStatus) { s.State = RecoveryFenceStateActive },
		"missing row":        func(s *RecoveryFenceStatus) { s.RowCount = 0 },
		"wrong payload type": func(s *RecoveryFenceStatus) { s.PayloadStorageClass = "text" },
		"wrong payload digest": func(s *RecoveryFenceStatus) {
			s.PayloadSHA256 = "not-canonical"
		},
		"negative generation": func(s *RecoveryFenceStatus) { negative := int64(-1); s.Generation = &negative },
		"active id":           func(s *RecoveryFenceStatus) { s.ActivationID = "must-be-null-when-inactive" },
		"future database":     func(s *RecoveryFenceStatus) { s.DatabaseSchemaVersion = 26 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := exact
			mutate(&candidate)
			if candidate.AllowsNormalBoot() {
				t.Fatal("non-exact fence authorized normal boot")
			}
		})
	}
}
