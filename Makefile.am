ACLOCAL_AMFLAGS = -I m4

SUBDIRS = third_party/t1ha src include mysql_engine $(EXPERIMENT_DIR)

protodir = $(prefix)/proto
proto_DATA = \
	proto/pinba.proto

scriptsdir = $(prefix)/scripts
scripts_DATA = \
	scripts/convert_mysqldump.php \
	scripts/default_tables/active.sql \
	scripts/default_tables/info.sql \
	scripts/default_tables/stats.sql \
	scripts/default_reports.sql \
	#
