############################################################
-include ../../Makefile.options
############################################################
include .env
BACKUP_DIR=./backups
BACKUP_FILE=$(BACKUP_DIR)/metadata_$(shell date +%Y%m%d_%H%M%S).dump
DB_NAME=metadata
PSQL_USER=user
workers?=4
file?=audio.ina_segments
file-to-db?=../../runner/target/release/file-to-db
filler?=../../runner/target/release/db-filler
########################################################################################################
## print usage information
help:
	@echo 'Usage:'
	@cat ${MAKEFILE_LIST} | grep -e "^## " -A 1 | grep -v '\-\-' | sed 's/^##//' | cut -f1 -d":" | \
		awk '{info=$$0; getline; print "  " $$0 ": " info;}' | column -t -s ':' | sort
########################################################################################################
run:
	docker compose up -d 
.PHONY: run
########################################################################################################
run/filler: 
	$(filler) --input=$(corpus_dir) --workers=$(workers) --audio-base=$(output_dir) --extensions=m4a
.PHONY: run/runner
run/filler/08khz: 
	$(filler) --input=$(corpus_dir)/08khz --workers=$(workers) --prefix 08khz --audio-base=$(output_dir) --extensions=m4a,wav,mp3
.PHONY: run/runner

run/filler/voxlingua: 
	$(filler) --input=$(voxlingua_dir) --workers=$(workers) --prefix voxlingua --audio-base=$(output_dir) --extensions=m4a,wav,mp3,ogg --source voxlingua
.PHONY: run/runner/voxlingua
run/filler/voxpopuli: 
	$(filler) --input=$(voxpopuli_dir) --workers=$(workers) --prefix voxpopuli --audio-base=$(output_dir) --extensions=m4a,wav,mp3,ogg --source voxpopuli
.PHONY: run/runner/voxpopuli
########################################################################################################
run/to-db: 
	$(file-to-db) --workers=$(workers) --input-base=$(output_dir) --names=$(file)
.PHONY: run/runner	
########################################################################################################
clean:
	docker compose down
.PHONY: clean
########################################################################################################
psql/metadata:
	docker compose exec -e PGPASSWORD=$(PSQL_PASS) metadata-db psql -U $(PSQL_USER) -h localhost -d $(DB_NAME)
.PHONY: psql/metadata
########################################################################################################
$(BACKUP_DIR):
	mkdir -p $(BACKUP_DIR)
# Backup database
backup/metadata: | $(BACKUP_DIR)
	docker compose exec -it -e PGPASSWORD=$(PSQL_PASS) metadata-db pg_dump -v -U $(PSQL_USER) -d $(DB_NAME) -F c -f /tmp/backup.dump
	docker compose cp metadata-db:/tmp/backup.dump $(BACKUP_FILE)
	docker compose exec metadata-db rm /tmp/backup.dump
	@echo "Backup saved to $(BACKUP_FILE)"
.PHONY: backup/metadata

########################################################################################################
# Restore database from a given file
restore/metadata:
ifndef restore_file
	$(error restore_file variable is required, e.g., make restore/metadata restore_file=./backups/metadata_20250818_123456.dump)
endif
	@echo -n "Restore from $(restore_file)? Are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]
	docker compose cp $(restore_file) metadata-db:/tmp/restore.dump
	docker compose exec -it -e PGPASSWORD=$(PSQL_PASS) metadata-db pg_restore -v -U $(PSQL_USER) -d $(DB_NAME) -c /tmp/restore.dump
	docker compose exec metadata-db rm /tmp/restore.dump
	@echo "Database restored from $(restore_file)"
.PHONY: restore/metadata
########################################################################################################
.EXPORT_ALL_VARIABLES:
