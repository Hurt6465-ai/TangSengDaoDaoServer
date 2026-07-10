package user

import (
	"strings"

	"github.com/gocraft/dbr/v2"
)

const googleIdentityProvider = "google"

// thirdIdentityModel 保存第三方平台的稳定用户标识。
// Google 必须使用 ID Token 中的 sub，不能使用可能变化的邮箱作为登录主键。
type thirdIdentityModel struct {
	ID             int64
	UID            string
	Provider       string
	ProviderUserID string
	Email          string
	EmailVerified  int
	DisplayName    string
	AvatarURL      string
}

// ensureThirdIdentityTable 让 Google 登录在关闭 SQL migration 时仍可使用。
// SQL 迁移里也使用 CREATE TABLE IF NOT EXISTS，因此两种初始化方式不会冲突。
func (d *DB) ensureThirdIdentityTable() error {
	_, err := d.session.UpdateBySql(`CREATE TABLE IF NOT EXISTS user_third_identity (
		id BIGINT NOT NULL AUTO_INCREMENT,
		uid VARCHAR(64) NOT NULL,
		provider VARCHAR(32) NOT NULL,
		provider_user_id VARCHAR(255) NOT NULL,
		email VARCHAR(255) NOT NULL DEFAULT '',
		email_verified TINYINT NOT NULL DEFAULT 0,
		display_name VARCHAR(255) NOT NULL DEFAULT '',
		avatar_url VARCHAR(1000) NOT NULL DEFAULT '',
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		UNIQUE KEY uk_third_identity_provider_user (provider, provider_user_id),
		UNIQUE KEY uk_third_identity_uid_provider (uid, provider),
		KEY idx_third_identity_email (email)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户第三方登录身份'`).Exec()
	return err
}

func (d *DB) queryThirdIdentity(provider, providerUserID string) (*thirdIdentityModel, error) {
	provider = strings.TrimSpace(provider)
	providerUserID = strings.TrimSpace(providerUserID)
	if provider == "" || providerUserID == "" {
		return nil, nil
	}
	var identity *thirdIdentityModel
	_, err := d.session.Select("id", "uid", "provider", "provider_user_id", "email", "email_verified", "display_name", "avatar_url").
		From("user_third_identity").
		Where("provider=? AND provider_user_id=?", provider, providerUserID).
		Limit(1).
		Load(&identity)
	return identity, err
}

func (d *DB) insertThirdIdentityTx(identity *thirdIdentityModel, tx *dbr.Tx) error {
	if identity == nil {
		return nil
	}
	_, err := tx.InsertInto("user_third_identity").
		Columns("uid", "provider", "provider_user_id", "email", "email_verified", "display_name", "avatar_url").
		Values(identity.UID, identity.Provider, identity.ProviderUserID, identity.Email, identity.EmailVerified, identity.DisplayName, identity.AvatarURL).
		Exec()
	return err
}

func (d *DB) updateThirdIdentityMetadata(provider, providerUserID, email, displayName, avatarURL string) error {
	_, err := d.session.Update("user_third_identity").
		SetMap(map[string]interface{}{
			"email":          strings.TrimSpace(email),
			"email_verified": 1,
			"display_name":   strings.TrimSpace(displayName),
			"avatar_url":     strings.TrimSpace(avatarURL),
		}).
		Where("provider=? AND provider_user_id=?", strings.TrimSpace(provider), strings.TrimSpace(providerUserID)).
		Exec()
	return err
}

func (d *DB) deleteThirdIdentity(provider, providerUserID string) error {
	_, err := d.session.DeleteFrom("user_third_identity").
		Where("provider=? AND provider_user_id=?", strings.TrimSpace(provider), strings.TrimSpace(providerUserID)).
		Exec()
	return err
}

func (d *DB) deleteThirdIdentitiesByUID(uid string) error {
	_, err := d.session.DeleteFrom("user_third_identity").Where("uid=?", strings.TrimSpace(uid)).Exec()
	return err
}

func (d *DB) queryByEmailExact(email string) (*Model, error) {
	email = strings.TrimSpace(email)
	if email == "" {
		return nil, nil
	}
	var model *Model
	_, err := d.session.Select("*").From("user").Where("email=? AND is_destroy=0", email).Limit(1).Load(&model)
	return model, err
}
