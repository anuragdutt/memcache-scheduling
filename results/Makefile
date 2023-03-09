copy_folders:
	mkdir -p $(EXPDIR)
	cp -r timing_list_default $(EXPDIR)
	cp -r timing_list_priority $(EXPDIR)
	@echo "Folders copied to $(EXPDIR)"
	rm -rf timing_list_default/*
	rm -rf timing_list_priority/*
	@echo "Folders cleared"

.PHONY: copy_folders
