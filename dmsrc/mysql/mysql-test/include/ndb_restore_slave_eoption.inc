######################################################
# By JBM 2006-03-08 So that the code is not repeated #
# in test cases and can be reused.                   #
######################################################

--exec $NDB_TOOLS_DIR/ndb_restore --no-defaults --ndb-connectstring="localhost:$NDBCLUSTER_PORT_SLAVE" -p 8 -b $the_backup_id -n 1 -m -r --print --print_meta $NDB_BACKUP_DIR/BACKUP/BACKUP-$the_backup_id >> $NDB_TOOLS_OUTPUT

--exec $NDB_TOOLS_DIR/ndb_restore --no-defaults --ndb-connectstring="localhost:$NDBCLUSTER_PORT_SLAVE" -p 8 -b $the_backup_id -n 2 -r -e --print --print_meta $NDB_BACKUP_DIR/BACKUP/BACKUP-$the_backup_id >> $NDB_TOOLS_OUTPUT



