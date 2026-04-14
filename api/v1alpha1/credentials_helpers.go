package v1alpha1

// UsesCredentials returns true when the spec uses per-role credential
// management rather than the legacy single-secret model.
func (s *MysqlFailoverGroupSpec) UsesCredentials() bool {
	return s.Credentials != nil
}

// EffectiveOperatorSecretName returns the secret name used for operator
// connections, regardless of whether legacy or credentials mode is active.
func (s *MysqlFailoverGroupSpec) EffectiveOperatorSecretName() string {
	if s.Credentials != nil {
		return s.Credentials.OperatorSecret
	}
	return s.SecretName
}

// EffectiveBackupSecretName returns the secret name used for backup and
// restore operations. In credentials mode it prefers the dedicated backup
// secret, falling back to the operator secret. In legacy mode it returns
// the single secret name.
func (s *MysqlFailoverGroupSpec) EffectiveBackupSecretName() string {
	if s.Credentials != nil {
		if s.Credentials.BackupSecret != "" {
			return s.Credentials.BackupSecret
		}
		return s.Credentials.OperatorSecret
	}
	return s.SecretName
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

	add(s.SecretName)
	if s.Credentials != nil {
		add(s.Credentials.OperatorSecret)
		add(s.Credentials.AppSecret)
		add(s.Credentials.ReadOnlySecret)
		add(s.Credentials.MonitorSecret)
		add(s.Credentials.BackupSecret)
	}
	return names
}
