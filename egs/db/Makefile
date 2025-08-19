############################################################
-include ../../Makefile.options
############################################################
include .env
BACKUP_DIR=./backups
BACKUP_FILE=$(BACKUP_DIR)/metadata_$(shell date +%Y%m%d_%H%M%S).dump
DB_NAME=metadata
PSQL_USER=user
workers?=4
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
########################################################################################################
clean:
	docker compose down
.PHONY: clean
########################################################################################################
psql/metadata:
	docker compose exec -e PGPASSWORD=$(PSQL_PASS) metadata-db psql -U $(PSQL_USER) -h localhost -d $(DB_NAME)
.PHONY: psql/metadata
########################################################################################################
# Backup database
backup/metadata:
	mkdir -p $(BACKUP_DIR)
	docker compose exec -e PGPASSWORD=$(PSQL_PASS) metadata-db pg_dump -U $(PSQL_USER) -d $(DB_NAME) -F c -f /tmp/backup.dump
	docker compose cp metadata-db:/tmp/backup.dump $(BACKUP_FILE)
	docker compose exec metadata-db rm /tmp/backup.dump
	@echo "Backup saved to $(BACKUP_FILE)"
.PHONY: backup/metadata

########################################################################################################
# Restore database from a given file
restore/metadata:
ifndef FILE
	$(error FILE variable is required, e.g., make restore/metadata FILE=./backups/metadata_20250818_123456.dump)
endif
	docker compose cp $(FILE) metadata-db:/tmp/restore.dump
	docker compose exec -e PGPASSWORD=$(PSQL_PASS) metadata-db pg_restore -U $(PSQL_USER) -d $(DB_NAME) -c /tmp/restore.dump
	docker compose exec metadata-db rm /tmp/restore.dump
	@echo "Database restored from $(FILE)"
.PHONY: restore/metadata
########################################################################################################
.EXPORT_ALL_VARIABLES:
