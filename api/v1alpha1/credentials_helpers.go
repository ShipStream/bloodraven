package v1alpha1

// EffectiveBackupSecretName returns the secret name used for backup (and
// backup verification) Jobs; restore Jobs use the operator secret. It
// prefers the dedicated backup secret, falling back to the operator secret.
func (s *MysqlFailoverGroupSpec) EffectiveBackupSecretName() string {
	if s.Credentials.BackupSecret != "" {
		return s.Credentials.BackupSecret
	}
	return s.Credentials.OperatorSecret
}

// AllReferencedSecretNames returns every distinct secret name referenced
// by the spec. Used by the secret watcher to trigger reconciliation when
// any credential secret changes.
func (s *MysqlFailoverGroupSpec) AllReferencedSecretNames() []string {
	seen := make(map[string]struct{})
	var names []string
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}

	add(s.Credentials.OperatorSecret)
	add(s.Credentials.AppSecret)
	add(s.Credentials.ReadOnlySecret)
	add(s.Credentials.MonitorSecret)
	add(s.Credentials.BackupSecret)
	if s.Backup != nil {
		for i := range s.Backup.Profiles {
			if enc := s.Backup.Profiles[i].Encryption; enc != nil {
				add(enc.PassphraseSecret.Name)
			}
		}
	}
	if s.InitFromBackup != nil && s.InitFromBackup.Decryption != nil {
		add(s.InitFromBackup.Decryption.PassphraseSecret.Name)
	}
	if s.RestoreInPlace != nil && s.RestoreInPlace.Decryption != nil {
		add(s.RestoreInPlace.Decryption.PassphraseSecret.Name)
	}
	return names
}

// EncryptionEnabled returns true when the profile has backup encryption
// configured.
func (p *BackupProfile) EncryptionEnabled() bool {
	return p != nil && p.Encryption != nil && p.Encryption.PassphraseSecret.Name != ""
}

// PassphraseSecretKeyOrDefault returns the Secret key holding the
// passphrase, defaulting to "passphrase" when unset.
func (r PassphraseSecretRef) PassphraseSecretKeyOrDefault() string {
	if r.Key == "" {
		return "passphrase"
	}
	return r.Key
}

// AlgorithmOrDefault returns the configured algorithm, defaulting to
// AES-256-GCM when the field is empty (common on older CRs that were
// created before Algorithm became optional).
func (s *BackupEncryptionSpec) AlgorithmOrDefault() string {
	if s == nil || s.Algorithm == "" {
		return "AES-256-GCM"
	}
	return s.Algorithm
}
