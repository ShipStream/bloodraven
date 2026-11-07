package v1alpha1

// EffectiveBackupSecretName returns the secret name used for backup (and
// backup verification) Jobs; restore Jobs use the operator secret. It
// prefers the dedicated backup secret, falling back to the operator secret.
func (s *MysqlFailoverGroupSpec) EffectiveBackupSecretName() string {
	if s.Credentials.BackupSecretName != "" {
		return s.Credentials.BackupSecretName
	}
	return s.Credentials.OperatorSecretName
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

	add(s.Credentials.OperatorSecretName)
	add(s.Credentials.AppSecretName)
	add(s.Credentials.ReadOnlySecretName)
	add(s.Credentials.MonitorSecretName)
	add(s.Credentials.BackupSecretName)
	if s.Backup != nil {
		for i := range s.Backup.Profiles {
			if enc := s.Backup.Profiles[i].Encryption; enc != nil {
				add(enc.PassphraseSecretKeyRef.Name)
			}
		}
	}
	if s.InitFromBackup != nil && s.InitFromBackup.Decryption != nil {
		add(s.InitFromBackup.Decryption.PassphraseSecretKeyRef.Name)
	}
	if s.RestoreInPlace != nil && s.RestoreInPlace.Decryption != nil {
		add(s.RestoreInPlace.Decryption.PassphraseSecretKeyRef.Name)
	}
	return names
}

// EncryptionEnabled returns true when the profile has backup encryption
// configured.
func (p *BackupProfile) EncryptionEnabled() bool {
	return p != nil && p.Encryption != nil && p.Encryption.PassphraseSecretKeyRef.Name != ""
}

// PassphraseSecretKeyOrDefault returns the Secret key holding the
// passphrase, defaulting to "passphrase" when unset.
func (r PassphraseSecretKeyRef) PassphraseSecretKeyOrDefault() string {
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

// KeyOrDefault returns the Secret key holding the Dragonfly password,
// defaulting to "password" when unset.
func (r DragonflyPasswordSecretKeyRef) KeyOrDefault() string {
	if r.Key == "" {
		return "password"
	}
	return r.Key
}
