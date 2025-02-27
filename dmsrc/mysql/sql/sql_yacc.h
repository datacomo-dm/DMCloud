/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     ABORT_SYM = 258,
     ACCESSIBLE_SYM = 259,
     ACTION = 260,
     ADD = 261,
     ADDDATE_SYM = 262,
     AFTER_SYM = 263,
     AGAINST = 264,
     AGGREGATE_SYM = 265,
     ALGORITHM_SYM = 266,
     ALL = 267,
     ALTER = 268,
     ANALYZE_SYM = 269,
     AND_AND_SYM = 270,
     AND_SYM = 271,
     ANY_SYM = 272,
     AS = 273,
     ASC = 274,
     ASCII_SYM = 275,
     ASENSITIVE_SYM = 276,
     AT_SYM = 277,
     AUTHORS_SYM = 278,
     AUTOEXTEND_SIZE_SYM = 279,
     AUTO_INC = 280,
     AVG_ROW_LENGTH = 281,
     AVG_SYM = 282,
     BACKUP_SYM = 283,
     BEFORE_SYM = 284,
     BEGIN_SYM = 285,
     BETWEEN_SYM = 286,
     BIGINT = 287,
     BINARY = 288,
     BINLOG_SYM = 289,
     BIN_NUM = 290,
     BIT_AND = 291,
     BIT_OR = 292,
     BIT_SYM = 293,
     BIT_XOR = 294,
     BLOB_SYM = 295,
     BLOCK_SYM = 296,
     BOOLEAN_SYM = 297,
     BOOL_SYM = 298,
     BOTH = 299,
     BTREE_SYM = 300,
     BY = 301,
     BYTE_SYM = 302,
     CACHE_SYM = 303,
     CALL_SYM = 304,
     CASCADE = 305,
     CASCADED = 306,
     CASE_SYM = 307,
     CAST_SYM = 308,
     CHAIN_SYM = 309,
     CHANGE = 310,
     CHANGED = 311,
     CHARSET = 312,
     CHAR_SYM = 313,
     CHECKSUM_SYM = 314,
     CHECK_SYM = 315,
     CIPHER_SYM = 316,
     CLIENT_SYM = 317,
     CLOSE_SYM = 318,
     COALESCE = 319,
     CODE_SYM = 320,
     COLLATE_SYM = 321,
     COLLATION_SYM = 322,
     COLUMNS = 323,
     COLUMN_SYM = 324,
     COMMENT_SYM = 325,
     COMMITTED_SYM = 326,
     COMMIT_SYM = 327,
     COMPACT_SYM = 328,
     COMPLETION_SYM = 329,
     COMPRESSED_SYM = 330,
     CONCURRENT = 331,
     CONDITION_SYM = 332,
     CONNECTION_SYM = 333,
     CONSISTENT_SYM = 334,
     CONSTRAINT = 335,
     CONTAINS_SYM = 336,
     CONTEXT_SYM = 337,
     CONTINUE_SYM = 338,
     CONTRIBUTORS_SYM = 339,
     CONVERT_SYM = 340,
     COUNT_SYM = 341,
     CPU_SYM = 342,
     CREATE = 343,
     CROSS = 344,
     CUBE_SYM = 345,
     CURDATE = 346,
     CURRENT_USER = 347,
     CURSOR_SYM = 348,
     CURTIME = 349,
     DATABASE = 350,
     DATABASES = 351,
     DATAFILE_SYM = 352,
     DATA_SYM = 353,
     DATETIME = 354,
     DATE_ADD_INTERVAL = 355,
     DATE_SUB_INTERVAL = 356,
     DATE_SYM = 357,
     DAY_HOUR_SYM = 358,
     DAY_MICROSECOND_SYM = 359,
     DAY_MINUTE_SYM = 360,
     DAY_SECOND_SYM = 361,
     DAY_SYM = 362,
     DEALLOCATE_SYM = 363,
     DECIMAL_NUM = 364,
     DECIMAL_SYM = 365,
     DECLARE_SYM = 366,
     DEFAULT = 367,
     DEFINER_SYM = 368,
     DELAYED_SYM = 369,
     DELAY_KEY_WRITE_SYM = 370,
     DELETE_SYM = 371,
     DESC = 372,
     DESCRIBE = 373,
     DES_KEY_FILE = 374,
     DETERMINISTIC_SYM = 375,
     DIRECTORY_SYM = 376,
     DISABLE_SYM = 377,
     DISCARD = 378,
     DISK_SYM = 379,
     DISTINCT = 380,
     DIV_SYM = 381,
     DOUBLE_SYM = 382,
     DO_SYM = 383,
     DROP = 384,
     DUAL_SYM = 385,
     DUMPFILE = 386,
     DUPLICATE_SYM = 387,
     DYNAMIC_SYM = 388,
     EACH_SYM = 389,
     ELSE = 390,
     ELSEIF_SYM = 391,
     ENABLE_SYM = 392,
     ENCLOSED = 393,
     END = 394,
     ENDS_SYM = 395,
     END_OF_INPUT = 396,
     ENGINES_SYM = 397,
     ENGINE_SYM = 398,
     ENUM = 399,
     EQ = 400,
     EQUAL_SYM = 401,
     ERRORS = 402,
     ESCAPED = 403,
     ESCAPE_SYM = 404,
     EVENTS_SYM = 405,
     EVENT_SYM = 406,
     EVERY_SYM = 407,
     EXECUTE_SYM = 408,
     EXISTS = 409,
     EXIT_SYM = 410,
     EXPANSION_SYM = 411,
     EXTENDED_SYM = 412,
     EXTENT_SIZE_SYM = 413,
     EXTRACT_SYM = 414,
     FALSE_SYM = 415,
     FAST_SYM = 416,
     FAULTS_SYM = 417,
     FETCH_SYM = 418,
     FILE_SYM = 419,
     FIRST_SYM = 420,
     FIXED_SYM = 421,
     FLOAT_NUM = 422,
     FLOAT_SYM = 423,
     FLUSH_SYM = 424,
     FORCE_SYM = 425,
     FOREIGN = 426,
     FOR_SYM = 427,
     FOUND_SYM = 428,
     FRAC_SECOND_SYM = 429,
     FROM = 430,
     FULL = 431,
     FULLTEXT_SYM = 432,
     FUNCTION_SYM = 433,
     GE = 434,
     GEOMETRYCOLLECTION = 435,
     GEOMETRY_SYM = 436,
     GET_FORMAT = 437,
     GLOBAL_SYM = 438,
     GRANT = 439,
     GRANTS = 440,
     GROUP_SYM = 441,
     GROUP_CONCAT_SYM = 442,
     GT_SYM = 443,
     HANDLER_SYM = 444,
     HASH_SYM = 445,
     HAVING = 446,
     HELP_SYM = 447,
     HEX_NUM = 448,
     HIGH_PRIORITY = 449,
     HOST_SYM = 450,
     HOSTS_SYM = 451,
     HOUR_MICROSECOND_SYM = 452,
     HOUR_MINUTE_SYM = 453,
     HOUR_SECOND_SYM = 454,
     HOUR_SYM = 455,
     IDENT = 456,
     IDENTIFIED_SYM = 457,
     IDENT_QUOTED = 458,
     IF = 459,
     IGNORE_SYM = 460,
     IMPORT = 461,
     INDEXES = 462,
     INDEX_SYM = 463,
     INFILE = 464,
     INITIAL_SIZE_SYM = 465,
     INNER_SYM = 466,
     INNOBASE_SYM = 467,
     INOUT_SYM = 468,
     INSENSITIVE_SYM = 469,
     INSERT = 470,
     INSERT_METHOD = 471,
     INSTALL_SYM = 472,
     INTERVAL_SYM = 473,
     INTO = 474,
     INT_SYM = 475,
     INVOKER_SYM = 476,
     IN_SYM = 477,
     IO_SYM = 478,
     IPC_SYM = 479,
     IS = 480,
     ISOLATION = 481,
     ISSUER_SYM = 482,
     ITERATE_SYM = 483,
     JOIN_SYM = 484,
     KEYS = 485,
     KEY_BLOCK_SIZE = 486,
     KEY_SYM = 487,
     KILL_SYM = 488,
     LANGUAGE_SYM = 489,
     LAST_SYM = 490,
     LE = 491,
     LEADING = 492,
     LEAVES = 493,
     LEAVE_SYM = 494,
     LEFT = 495,
     LESS_SYM = 496,
     LEVEL_SYM = 497,
     LEX_HOSTNAME = 498,
     LIKE = 499,
     LIMIT = 500,
     LINEAR_SYM = 501,
     LINES = 502,
     LINESTRING = 503,
     LIST_SYM = 504,
     LOAD = 505,
     LOCAL_SYM = 506,
     LOCATOR_SYM = 507,
     LOCKS_SYM = 508,
     LOCK_SYM = 509,
     LOGFILE_SYM = 510,
     LOGS_SYM = 511,
     LONGBLOB = 512,
     LONGTEXT = 513,
     LONG_NUM = 514,
     LONG_SYM = 515,
     LOOP_SYM = 516,
     LOW_PRIORITY = 517,
     LT = 518,
     MASTER_CONNECT_RETRY_SYM = 519,
     MASTER_HOST_SYM = 520,
     MASTER_LOG_FILE_SYM = 521,
     MASTER_LOG_POS_SYM = 522,
     MASTER_PASSWORD_SYM = 523,
     MASTER_PORT_SYM = 524,
     MASTER_SERVER_ID_SYM = 525,
     MASTER_SSL_CAPATH_SYM = 526,
     MASTER_SSL_CA_SYM = 527,
     MASTER_SSL_CERT_SYM = 528,
     MASTER_SSL_CIPHER_SYM = 529,
     MASTER_SSL_KEY_SYM = 530,
     MASTER_SSL_SYM = 531,
     MASTER_SSL_VERIFY_SERVER_CERT_SYM = 532,
     MASTER_SYM = 533,
     MASTER_USER_SYM = 534,
     MATCH = 535,
     MAX_CONNECTIONS_PER_HOUR = 536,
     MAX_QUERIES_PER_HOUR = 537,
     MAX_ROWS = 538,
     MAX_SIZE_SYM = 539,
     MAX_SYM = 540,
     MAX_UPDATES_PER_HOUR = 541,
     MAX_USER_CONNECTIONS_SYM = 542,
     MAX_VALUE_SYM = 543,
     MEDIUMBLOB = 544,
     MEDIUMINT = 545,
     MEDIUMTEXT = 546,
     MEDIUM_SYM = 547,
     MEMORY_SYM = 548,
     MERGE_SYM = 549,
     MICROSECOND_SYM = 550,
     MIGRATE_SYM = 551,
     MINUTE_MICROSECOND_SYM = 552,
     MINUTE_SECOND_SYM = 553,
     MINUTE_SYM = 554,
     MIN_ROWS = 555,
     MIN_SYM = 556,
     MODE_SYM = 557,
     MODIFIES_SYM = 558,
     MODIFY_SYM = 559,
     MOD_SYM = 560,
     MONTH_SYM = 561,
     MULTILINESTRING = 562,
     MULTIPOINT = 563,
     MULTIPOLYGON = 564,
     MUTEX_SYM = 565,
     NAMES_SYM = 566,
     NAME_SYM = 567,
     NATIONAL_SYM = 568,
     NATURAL = 569,
     NCHAR_STRING = 570,
     NCHAR_SYM = 571,
     NDBCLUSTER_SYM = 572,
     NE = 573,
     NEG = 574,
     NEW_SYM = 575,
     NEXT_SYM = 576,
     NODEGROUP_SYM = 577,
     NONE_SYM = 578,
     NOT2_SYM = 579,
     NOT_SYM = 580,
     NOW_SYM = 581,
     NO_SYM = 582,
     NO_WAIT_SYM = 583,
     NO_WRITE_TO_BINLOG = 584,
     NULL_SYM = 585,
     NUM = 586,
     NUMERIC_SYM = 587,
     NVARCHAR_SYM = 588,
     OFFSET_SYM = 589,
     OLD_PASSWORD = 590,
     ON = 591,
     ONE_SHOT_SYM = 592,
     ONE_SYM = 593,
     OPEN_SYM = 594,
     OPTIMIZE = 595,
     OPTIONS_SYM = 596,
     OPTION = 597,
     OPTIONALLY = 598,
     OR2_SYM = 599,
     ORDER_SYM = 600,
     OR_OR_SYM = 601,
     OR_SYM = 602,
     OUTER = 603,
     OUTFILE = 604,
     OUT_SYM = 605,
     OWNER_SYM = 606,
     PACK_KEYS_SYM = 607,
     PAGE_SYM = 608,
     PAGE_CHECKSUM_SYM = 609,
     PARAM_MARKER = 610,
     PARSER_SYM = 611,
     PARTIAL = 612,
     PARTITIONING_SYM = 613,
     PARTITIONS_SYM = 614,
     PARTITION_SYM = 615,
     PASSWORD = 616,
     PHASE_SYM = 617,
     PLUGINS_SYM = 618,
     PLUGIN_SYM = 619,
     POINT_SYM = 620,
     POLYGON = 621,
     PORT_SYM = 622,
     POSITION_SYM = 623,
     PRECISION = 624,
     PREPARE_SYM = 625,
     PRESERVE_SYM = 626,
     PREV_SYM = 627,
     PRIMARY_SYM = 628,
     PRIVILEGES = 629,
     PROCEDURE = 630,
     PROCESS = 631,
     PROCESSLIST_SYM = 632,
     PROFILE_SYM = 633,
     PROFILES_SYM = 634,
     PURGE = 635,
     QUARTER_SYM = 636,
     QUERY_SYM = 637,
     QUICK = 638,
     RANGE_SYM = 639,
     READS_SYM = 640,
     READ_ONLY_SYM = 641,
     READ_SYM = 642,
     READ_WRITE_SYM = 643,
     REAL = 644,
     REBUILD_SYM = 645,
     RECOVER_SYM = 646,
     REDOFILE_SYM = 647,
     REDO_BUFFER_SIZE_SYM = 648,
     REDUNDANT_SYM = 649,
     REFERENCES = 650,
     REGEXP = 651,
     RELAY_LOG_FILE_SYM = 652,
     RELAY_LOG_POS_SYM = 653,
     RELAY_THREAD = 654,
     RELEASE_SYM = 655,
     RELOAD = 656,
     REMOVE_SYM = 657,
     RENAME = 658,
     REORGANIZE_SYM = 659,
     REPAIR = 660,
     REPEATABLE_SYM = 661,
     REPEAT_SYM = 662,
     REPLACE = 663,
     REPLICATION = 664,
     REQUIRE_SYM = 665,
     RESET_SYM = 666,
     RESOURCES = 667,
     RESTORE_SYM = 668,
     RESTRICT = 669,
     RESUME_SYM = 670,
     RETURNS_SYM = 671,
     RETURN_SYM = 672,
     REVOKE = 673,
     RIGHT = 674,
     ROLLBACK_SYM = 675,
     ROLLUP_SYM = 676,
     ROUTINE_SYM = 677,
     ROWS_SYM = 678,
     ROW_FORMAT_SYM = 679,
     ROW_SYM = 680,
     RTREE_SYM = 681,
     SAVEPOINT_SYM = 682,
     SCHEDULE_SYM = 683,
     SECOND_MICROSECOND_SYM = 684,
     SECOND_SYM = 685,
     SECURITY_SYM = 686,
     SELECT_SYM = 687,
     SENSITIVE_SYM = 688,
     SEPARATOR_SYM = 689,
     SERIALIZABLE_SYM = 690,
     SERIAL_SYM = 691,
     SESSION_SYM = 692,
     SERVER_SYM = 693,
     SERVER_OPTIONS = 694,
     SET = 695,
     SET_VAR = 696,
     SHARE_SYM = 697,
     SHIFT_LEFT = 698,
     SHIFT_RIGHT = 699,
     SHOW = 700,
     SHUTDOWN = 701,
     SIGNED_SYM = 702,
     SIMPLE_SYM = 703,
     SLAVE = 704,
     SMALLINT = 705,
     SNAPSHOT_SYM = 706,
     SOCKET_SYM = 707,
     SONAME_SYM = 708,
     SOUNDS_SYM = 709,
     SOURCE_SYM = 710,
     SPATIAL_SYM = 711,
     SPECIFIC_SYM = 712,
     SQLEXCEPTION_SYM = 713,
     SQLSTATE_SYM = 714,
     SQLWARNING_SYM = 715,
     SQL_BIG_RESULT = 716,
     SQL_BUFFER_RESULT = 717,
     SQL_CACHE_SYM = 718,
     SQL_CALC_FOUND_ROWS = 719,
     SQL_NO_CACHE_SYM = 720,
     SQL_SMALL_RESULT = 721,
     SQL_SYM = 722,
     SQL_THREAD = 723,
     SSL_SYM = 724,
     STARTING = 725,
     STARTS_SYM = 726,
     START_SYM = 727,
     STATUS_SYM = 728,
     STDDEV_SAMP_SYM = 729,
     STD_SYM = 730,
     STOP_SYM = 731,
     STORAGE_SYM = 732,
     STRAIGHT_JOIN = 733,
     STRING_SYM = 734,
     SUBDATE_SYM = 735,
     SUBJECT_SYM = 736,
     SUBPARTITIONS_SYM = 737,
     SUBPARTITION_SYM = 738,
     SUBSTRING = 739,
     SUM_SYM = 740,
     SUPER_SYM = 741,
     SUSPEND_SYM = 742,
     SWAPS_SYM = 743,
     SWITCHES_SYM = 744,
     SYSDATE = 745,
     TABLES = 746,
     TABLESPACE = 747,
     TABLE_REF_PRIORITY = 748,
     TABLE_SYM = 749,
     TABLE_CHECKSUM_SYM = 750,
     TEMPORARY = 751,
     TEMPTABLE_SYM = 752,
     TERMINATED = 753,
     TEXT_STRING = 754,
     TEXT_SYM = 755,
     THAN_SYM = 756,
     THEN_SYM = 757,
     TIMESTAMP = 758,
     TIMESTAMP_ADD = 759,
     TIMESTAMP_DIFF = 760,
     TIME_SYM = 761,
     TINYBLOB = 762,
     TINYINT = 763,
     TINYTEXT = 764,
     TO_SYM = 765,
     TRAILING = 766,
     TRANSACTION_SYM = 767,
     TRANSACTIONAL_SYM = 768,
     TRIGGERS_SYM = 769,
     TRIGGER_SYM = 770,
     TRIM = 771,
     TRUE_SYM = 772,
     TRUNCATE_SYM = 773,
     TYPES_SYM = 774,
     TYPE_SYM = 775,
     UDF_RETURNS_SYM = 776,
     ULONGLONG_NUM = 777,
     UNCOMMITTED_SYM = 778,
     UNDEFINED_SYM = 779,
     UNDERSCORE_CHARSET = 780,
     UNDOFILE_SYM = 781,
     UNDO_BUFFER_SIZE_SYM = 782,
     UNDO_SYM = 783,
     UNICODE_SYM = 784,
     UNINSTALL_SYM = 785,
     UNION_SYM = 786,
     UNIQUE_SYM = 787,
     UNKNOWN_SYM = 788,
     UNLOCK_SYM = 789,
     UNSIGNED = 790,
     UNTIL_SYM = 791,
     UPDATE_SYM = 792,
     UPGRADE_SYM = 793,
     USAGE = 794,
     USER = 795,
     USE_FRM = 796,
     USE_SYM = 797,
     USING = 798,
     UTC_DATE_SYM = 799,
     UTC_TIMESTAMP_SYM = 800,
     UTC_TIME_SYM = 801,
     VALUES = 802,
     VALUE_SYM = 803,
     VARBINARY = 804,
     VARCHAR = 805,
     VARIABLES = 806,
     VARIANCE_SYM = 807,
     VARYING = 808,
     VAR_SAMP_SYM = 809,
     VIEW_SYM = 810,
     WAIT_SYM = 811,
     WARNINGS = 812,
     WEEK_SYM = 813,
     WHEN_SYM = 814,
     WHERE = 815,
     WHILE_SYM = 816,
     WITH = 817,
     WORK_SYM = 818,
     WRAPPER_SYM = 819,
     WRITE_SYM = 820,
     X509_SYM = 821,
     XA_SYM = 822,
     XOR = 823,
     YEAR_MONTH_SYM = 824,
     YEAR_SYM = 825,
     ZEROFILL = 826
   };
#endif
/* Tokens.  */
#define ABORT_SYM 258
#define ACCESSIBLE_SYM 259
#define ACTION 260
#define ADD 261
#define ADDDATE_SYM 262
#define AFTER_SYM 263
#define AGAINST 264
#define AGGREGATE_SYM 265
#define ALGORITHM_SYM 266
#define ALL 267
#define ALTER 268
#define ANALYZE_SYM 269
#define AND_AND_SYM 270
#define AND_SYM 271
#define ANY_SYM 272
#define AS 273
#define ASC 274
#define ASCII_SYM 275
#define ASENSITIVE_SYM 276
#define AT_SYM 277
#define AUTHORS_SYM 278
#define AUTOEXTEND_SIZE_SYM 279
#define AUTO_INC 280
#define AVG_ROW_LENGTH 281
#define AVG_SYM 282
#define BACKUP_SYM 283
#define BEFORE_SYM 284
#define BEGIN_SYM 285
#define BETWEEN_SYM 286
#define BIGINT 287
#define BINARY 288
#define BINLOG_SYM 289
#define BIN_NUM 290
#define BIT_AND 291
#define BIT_OR 292
#define BIT_SYM 293
#define BIT_XOR 294
#define BLOB_SYM 295
#define BLOCK_SYM 296
#define BOOLEAN_SYM 297
#define BOOL_SYM 298
#define BOTH 299
#define BTREE_SYM 300
#define BY 301
#define BYTE_SYM 302
#define CACHE_SYM 303
#define CALL_SYM 304
#define CASCADE 305
#define CASCADED 306
#define CASE_SYM 307
#define CAST_SYM 308
#define CHAIN_SYM 309
#define CHANGE 310
#define CHANGED 311
#define CHARSET 312
#define CHAR_SYM 313
#define CHECKSUM_SYM 314
#define CHECK_SYM 315
#define CIPHER_SYM 316
#define CLIENT_SYM 317
#define CLOSE_SYM 318
#define COALESCE 319
#define CODE_SYM 320
#define COLLATE_SYM 321
#define COLLATION_SYM 322
#define COLUMNS 323
#define COLUMN_SYM 324
#define COMMENT_SYM 325
#define COMMITTED_SYM 326
#define COMMIT_SYM 327
#define COMPACT_SYM 328
#define COMPLETION_SYM 329
#define COMPRESSED_SYM 330
#define CONCURRENT 331
#define CONDITION_SYM 332
#define CONNECTION_SYM 333
#define CONSISTENT_SYM 334
#define CONSTRAINT 335
#define CONTAINS_SYM 336
#define CONTEXT_SYM 337
#define CONTINUE_SYM 338
#define CONTRIBUTORS_SYM 339
#define CONVERT_SYM 340
#define COUNT_SYM 341
#define CPU_SYM 342
#define CREATE 343
#define CROSS 344
#define CUBE_SYM 345
#define CURDATE 346
#define CURRENT_USER 347
#define CURSOR_SYM 348
#define CURTIME 349
#define DATABASE 350
#define DATABASES 351
#define DATAFILE_SYM 352
#define DATA_SYM 353
#define DATETIME 354
#define DATE_ADD_INTERVAL 355
#define DATE_SUB_INTERVAL 356
#define DATE_SYM 357
#define DAY_HOUR_SYM 358
#define DAY_MICROSECOND_SYM 359
#define DAY_MINUTE_SYM 360
#define DAY_SECOND_SYM 361
#define DAY_SYM 362
#define DEALLOCATE_SYM 363
#define DECIMAL_NUM 364
#define DECIMAL_SYM 365
#define DECLARE_SYM 366
#define DEFAULT 367
#define DEFINER_SYM 368
#define DELAYED_SYM 369
#define DELAY_KEY_WRITE_SYM 370
#define DELETE_SYM 371
#define DESC 372
#define DESCRIBE 373
#define DES_KEY_FILE 374
#define DETERMINISTIC_SYM 375
#define DIRECTORY_SYM 376
#define DISABLE_SYM 377
#define DISCARD 378
#define DISK_SYM 379
#define DISTINCT 380
#define DIV_SYM 381
#define DOUBLE_SYM 382
#define DO_SYM 383
#define DROP 384
#define DUAL_SYM 385
#define DUMPFILE 386
#define DUPLICATE_SYM 387
#define DYNAMIC_SYM 388
#define EACH_SYM 389
#define ELSE 390
#define ELSEIF_SYM 391
#define ENABLE_SYM 392
#define ENCLOSED 393
#define END 394
#define ENDS_SYM 395
#define END_OF_INPUT 396
#define ENGINES_SYM 397
#define ENGINE_SYM 398
#define ENUM 399
#define EQ 400
#define EQUAL_SYM 401
#define ERRORS 402
#define ESCAPED 403
#define ESCAPE_SYM 404
#define EVENTS_SYM 405
#define EVENT_SYM 406
#define EVERY_SYM 407
#define EXECUTE_SYM 408
#define EXISTS 409
#define EXIT_SYM 410
#define EXPANSION_SYM 411
#define EXTENDED_SYM 412
#define EXTENT_SIZE_SYM 413
#define EXTRACT_SYM 414
#define FALSE_SYM 415
#define FAST_SYM 416
#define FAULTS_SYM 417
#define FETCH_SYM 418
#define FILE_SYM 419
#define FIRST_SYM 420
#define FIXED_SYM 421
#define FLOAT_NUM 422
#define FLOAT_SYM 423
#define FLUSH_SYM 424
#define FORCE_SYM 425
#define FOREIGN 426
#define FOR_SYM 427
#define FOUND_SYM 428
#define FRAC_SECOND_SYM 429
#define FROM 430
#define FULL 431
#define FULLTEXT_SYM 432
#define FUNCTION_SYM 433
#define GE 434
#define GEOMETRYCOLLECTION 435
#define GEOMETRY_SYM 436
#define GET_FORMAT 437
#define GLOBAL_SYM 438
#define GRANT 439
#define GRANTS 440
#define GROUP_SYM 441
#define GROUP_CONCAT_SYM 442
#define GT_SYM 443
#define HANDLER_SYM 444
#define HASH_SYM 445
#define HAVING 446
#define HELP_SYM 447
#define HEX_NUM 448
#define HIGH_PRIORITY 449
#define HOST_SYM 450
#define HOSTS_SYM 451
#define HOUR_MICROSECOND_SYM 452
#define HOUR_MINUTE_SYM 453
#define HOUR_SECOND_SYM 454
#define HOUR_SYM 455
#define IDENT 456
#define IDENTIFIED_SYM 457
#define IDENT_QUOTED 458
#define IF 459
#define IGNORE_SYM 460
#define IMPORT 461
#define INDEXES 462
#define INDEX_SYM 463
#define INFILE 464
#define INITIAL_SIZE_SYM 465
#define INNER_SYM 466
#define INNOBASE_SYM 467
#define INOUT_SYM 468
#define INSENSITIVE_SYM 469
#define INSERT 470
#define INSERT_METHOD 471
#define INSTALL_SYM 472
#define INTERVAL_SYM 473
#define INTO 474
#define INT_SYM 475
#define INVOKER_SYM 476
#define IN_SYM 477
#define IO_SYM 478
#define IPC_SYM 479
#define IS 480
#define ISOLATION 481
#define ISSUER_SYM 482
#define ITERATE_SYM 483
#define JOIN_SYM 484
#define KEYS 485
#define KEY_BLOCK_SIZE 486
#define KEY_SYM 487
#define KILL_SYM 488
#define LANGUAGE_SYM 489
#define LAST_SYM 490
#define LE 491
#define LEADING 492
#define LEAVES 493
#define LEAVE_SYM 494
#define LEFT 495
#define LESS_SYM 496
#define LEVEL_SYM 497
#define LEX_HOSTNAME 498
#define LIKE 499
#define LIMIT 500
#define LINEAR_SYM 501
#define LINES 502
#define LINESTRING 503
#define LIST_SYM 504
#define LOAD 505
#define LOCAL_SYM 506
#define LOCATOR_SYM 507
#define LOCKS_SYM 508
#define LOCK_SYM 509
#define LOGFILE_SYM 510
#define LOGS_SYM 511
#define LONGBLOB 512
#define LONGTEXT 513
#define LONG_NUM 514
#define LONG_SYM 515
#define LOOP_SYM 516
#define LOW_PRIORITY 517
#define LT 518
#define MASTER_CONNECT_RETRY_SYM 519
#define MASTER_HOST_SYM 520
#define MASTER_LOG_FILE_SYM 521
#define MASTER_LOG_POS_SYM 522
#define MASTER_PASSWORD_SYM 523
#define MASTER_PORT_SYM 524
#define MASTER_SERVER_ID_SYM 525
#define MASTER_SSL_CAPATH_SYM 526
#define MASTER_SSL_CA_SYM 527
#define MASTER_SSL_CERT_SYM 528
#define MASTER_SSL_CIPHER_SYM 529
#define MASTER_SSL_KEY_SYM 530
#define MASTER_SSL_SYM 531
#define MASTER_SSL_VERIFY_SERVER_CERT_SYM 532
#define MASTER_SYM 533
#define MASTER_USER_SYM 534
#define MATCH 535
#define MAX_CONNECTIONS_PER_HOUR 536
#define MAX_QUERIES_PER_HOUR 537
#define MAX_ROWS 538
#define MAX_SIZE_SYM 539
#define MAX_SYM 540
#define MAX_UPDATES_PER_HOUR 541
#define MAX_USER_CONNECTIONS_SYM 542
#define MAX_VALUE_SYM 543
#define MEDIUMBLOB 544
#define MEDIUMINT 545
#define MEDIUMTEXT 546
#define MEDIUM_SYM 547
#define MEMORY_SYM 548
#define MERGE_SYM 549
#define MICROSECOND_SYM 550
#define MIGRATE_SYM 551
#define MINUTE_MICROSECOND_SYM 552
#define MINUTE_SECOND_SYM 553
#define MINUTE_SYM 554
#define MIN_ROWS 555
#define MIN_SYM 556
#define MODE_SYM 557
#define MODIFIES_SYM 558
#define MODIFY_SYM 559
#define MOD_SYM 560
#define MONTH_SYM 561
#define MULTILINESTRING 562
#define MULTIPOINT 563
#define MULTIPOLYGON 564
#define MUTEX_SYM 565
#define NAMES_SYM 566
#define NAME_SYM 567
#define NATIONAL_SYM 568
#define NATURAL 569
#define NCHAR_STRING 570
#define NCHAR_SYM 571
#define NDBCLUSTER_SYM 572
#define NE 573
#define NEG 574
#define NEW_SYM 575
#define NEXT_SYM 576
#define NODEGROUP_SYM 577
#define NONE_SYM 578
#define NOT2_SYM 579
#define NOT_SYM 580
#define NOW_SYM 581
#define NO_SYM 582
#define NO_WAIT_SYM 583
#define NO_WRITE_TO_BINLOG 584
#define NULL_SYM 585
#define NUM 586
#define NUMERIC_SYM 587
#define NVARCHAR_SYM 588
#define OFFSET_SYM 589
#define OLD_PASSWORD 590
#define ON 591
#define ONE_SHOT_SYM 592
#define ONE_SYM 593
#define OPEN_SYM 594
#define OPTIMIZE 595
#define OPTIONS_SYM 596
#define OPTION 597
#define OPTIONALLY 598
#define OR2_SYM 599
#define ORDER_SYM 600
#define OR_OR_SYM 601
#define OR_SYM 602
#define OUTER 603
#define OUTFILE 604
#define OUT_SYM 605
#define OWNER_SYM 606
#define PACK_KEYS_SYM 607
#define PAGE_SYM 608
#define PAGE_CHECKSUM_SYM 609
#define PARAM_MARKER 610
#define PARSER_SYM 611
#define PARTIAL 612
#define PARTITIONING_SYM 613
#define PARTITIONS_SYM 614
#define PARTITION_SYM 615
#define PASSWORD 616
#define PHASE_SYM 617
#define PLUGINS_SYM 618
#define PLUGIN_SYM 619
#define POINT_SYM 620
#define POLYGON 621
#define PORT_SYM 622
#define POSITION_SYM 623
#define PRECISION 624
#define PREPARE_SYM 625
#define PRESERVE_SYM 626
#define PREV_SYM 627
#define PRIMARY_SYM 628
#define PRIVILEGES 629
#define PROCEDURE 630
#define PROCESS 631
#define PROCESSLIST_SYM 632
#define PROFILE_SYM 633
#define PROFILES_SYM 634
#define PURGE 635
#define QUARTER_SYM 636
#define QUERY_SYM 637
#define QUICK 638
#define RANGE_SYM 639
#define READS_SYM 640
#define READ_ONLY_SYM 641
#define READ_SYM 642
#define READ_WRITE_SYM 643
#define REAL 644
#define REBUILD_SYM 645
#define RECOVER_SYM 646
#define REDOFILE_SYM 647
#define REDO_BUFFER_SIZE_SYM 648
#define REDUNDANT_SYM 649
#define REFERENCES 650
#define REGEXP 651
#define RELAY_LOG_FILE_SYM 652
#define RELAY_LOG_POS_SYM 653
#define RELAY_THREAD 654
#define RELEASE_SYM 655
#define RELOAD 656
#define REMOVE_SYM 657
#define RENAME 658
#define REORGANIZE_SYM 659
#define REPAIR 660
#define REPEATABLE_SYM 661
#define REPEAT_SYM 662
#define REPLACE 663
#define REPLICATION 664
#define REQUIRE_SYM 665
#define RESET_SYM 666
#define RESOURCES 667
#define RESTORE_SYM 668
#define RESTRICT 669
#define RESUME_SYM 670
#define RETURNS_SYM 671
#define RETURN_SYM 672
#define REVOKE 673
#define RIGHT 674
#define ROLLBACK_SYM 675
#define ROLLUP_SYM 676
#define ROUTINE_SYM 677
#define ROWS_SYM 678
#define ROW_FORMAT_SYM 679
#define ROW_SYM 680
#define RTREE_SYM 681
#define SAVEPOINT_SYM 682
#define SCHEDULE_SYM 683
#define SECOND_MICROSECOND_SYM 684
#define SECOND_SYM 685
#define SECURITY_SYM 686
#define SELECT_SYM 687
#define SENSITIVE_SYM 688
#define SEPARATOR_SYM 689
#define SERIALIZABLE_SYM 690
#define SERIAL_SYM 691
#define SESSION_SYM 692
#define SERVER_SYM 693
#define SERVER_OPTIONS 694
#define SET 695
#define SET_VAR 696
#define SHARE_SYM 697
#define SHIFT_LEFT 698
#define SHIFT_RIGHT 699
#define SHOW 700
#define SHUTDOWN 701
#define SIGNED_SYM 702
#define SIMPLE_SYM 703
#define SLAVE 704
#define SMALLINT 705
#define SNAPSHOT_SYM 706
#define SOCKET_SYM 707
#define SONAME_SYM 708
#define SOUNDS_SYM 709
#define SOURCE_SYM 710
#define SPATIAL_SYM 711
#define SPECIFIC_SYM 712
#define SQLEXCEPTION_SYM 713
#define SQLSTATE_SYM 714
#define SQLWARNING_SYM 715
#define SQL_BIG_RESULT 716
#define SQL_BUFFER_RESULT 717
#define SQL_CACHE_SYM 718
#define SQL_CALC_FOUND_ROWS 719
#define SQL_NO_CACHE_SYM 720
#define SQL_SMALL_RESULT 721
#define SQL_SYM 722
#define SQL_THREAD 723
#define SSL_SYM 724
#define STARTING 725
#define STARTS_SYM 726
#define START_SYM 727
#define STATUS_SYM 728
#define STDDEV_SAMP_SYM 729
#define STD_SYM 730
#define STOP_SYM 731
#define STORAGE_SYM 732
#define STRAIGHT_JOIN 733
#define STRING_SYM 734
#define SUBDATE_SYM 735
#define SUBJECT_SYM 736
#define SUBPARTITIONS_SYM 737
#define SUBPARTITION_SYM 738
#define SUBSTRING 739
#define SUM_SYM 740
#define SUPER_SYM 741
#define SUSPEND_SYM 742
#define SWAPS_SYM 743
#define SWITCHES_SYM 744
#define SYSDATE 745
#define TABLES 746
#define TABLESPACE 747
#define TABLE_REF_PRIORITY 748
#define TABLE_SYM 749
#define TABLE_CHECKSUM_SYM 750
#define TEMPORARY 751
#define TEMPTABLE_SYM 752
#define TERMINATED 753
#define TEXT_STRING 754
#define TEXT_SYM 755
#define THAN_SYM 756
#define THEN_SYM 757
#define TIMESTAMP 758
#define TIMESTAMP_ADD 759
#define TIMESTAMP_DIFF 760
#define TIME_SYM 761
#define TINYBLOB 762
#define TINYINT 763
#define TINYTEXT 764
#define TO_SYM 765
#define TRAILING 766
#define TRANSACTION_SYM 767
#define TRANSACTIONAL_SYM 768
#define TRIGGERS_SYM 769
#define TRIGGER_SYM 770
#define TRIM 771
#define TRUE_SYM 772
#define TRUNCATE_SYM 773
#define TYPES_SYM 774
#define TYPE_SYM 775
#define UDF_RETURNS_SYM 776
#define ULONGLONG_NUM 777
#define UNCOMMITTED_SYM 778
#define UNDEFINED_SYM 779
#define UNDERSCORE_CHARSET 780
#define UNDOFILE_SYM 781
#define UNDO_BUFFER_SIZE_SYM 782
#define UNDO_SYM 783
#define UNICODE_SYM 784
#define UNINSTALL_SYM 785
#define UNION_SYM 786
#define UNIQUE_SYM 787
#define UNKNOWN_SYM 788
#define UNLOCK_SYM 789
#define UNSIGNED 790
#define UNTIL_SYM 791
#define UPDATE_SYM 792
#define UPGRADE_SYM 793
#define USAGE 794
#define USER 795
#define USE_FRM 796
#define USE_SYM 797
#define USING 798
#define UTC_DATE_SYM 799
#define UTC_TIMESTAMP_SYM 800
#define UTC_TIME_SYM 801
#define VALUES 802
#define VALUE_SYM 803
#define VARBINARY 804
#define VARCHAR 805
#define VARIABLES 806
#define VARIANCE_SYM 807
#define VARYING 808
#define VAR_SAMP_SYM 809
#define VIEW_SYM 810
#define WAIT_SYM 811
#define WARNINGS 812
#define WEEK_SYM 813
#define WHEN_SYM 814
#define WHERE 815
#define WHILE_SYM 816
#define WITH 817
#define WORK_SYM 818
#define WRAPPER_SYM 819
#define WRITE_SYM 820
#define X509_SYM 821
#define XA_SYM 822
#define XOR 823
#define YEAR_MONTH_SYM 824
#define YEAR_SYM 825
#define ZEROFILL 826




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 462 "sql_yacc.yy"
{
  int  num;
  ulong ulong_num;
  ulonglong ulonglong_number;
  longlong longlong_number;
  LEX_STRING lex_str;
  LEX_STRING *lex_str_ptr;
  LEX_SYMBOL symbol;
  Table_ident *table;
  char *simple_string;
  Item *item;
  Item_num *item_num;
  List<Item> *item_list;
  List<String> *string_list;
  String *string;
  Key_part_spec *key_part;
  TABLE_LIST *table_list;
  udf_func *udf;
  LEX_USER *lex_user;
  struct sys_var_with_base variable;
  enum enum_var_type var_type;
  Key::Keytype key_type;
  enum ha_key_alg key_alg;
  handlerton *db_type;
  enum row_type row_type;
  enum ha_rkey_function ha_rkey_mode;
  enum enum_tx_isolation tx_isolation;
  enum Cast_target cast_type;
  enum Item_udftype udf_type;
  enum ha_choice choice;
  CHARSET_INFO *charset;
  thr_lock_type lock_type;
  interval_type interval, interval_time_st;
  timestamp_type date_time_type;
  st_select_lex *select_lex;
  chooser_compare_func_creator boolfunc2creator;
  struct sp_cond_type *spcondtype;
  struct { int vars, conds, hndlrs, curs; } spblock;
  sp_name *spname;
  struct st_lex *lex;
  sp_head *sphead;
  struct p_elem_val *p_elem_value;
  enum index_hint_type index_hint;
}
/* Line 1489 of yacc.c.  */
#line 1236 "sql_yacc.h"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



