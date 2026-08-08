ALTER TABLE plugin_installations ADD COLUMN contract_json BLOB;
ALTER TABLE plugin_installations ADD COLUMN contract_digest TEXT NOT NULL DEFAULT '';
