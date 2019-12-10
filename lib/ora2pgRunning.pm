@rem = '--*-Perl-*--
@echo off
if "%OS%" == "Windows_NT" goto WinNT
perl -x -S "%0" %1 %2 %3 %4 %5 %6 %7 %8 %9
goto endofperl
:WinNT
perl -x -S %0 %*
if NOT "%COMSPEC%" == "%SystemRoot%\system32\cmd.exe" goto endofperl
if %errorlevel% == 9009 echo You do not have Perl in your PATH.
if errorlevel 1 goto script_failed_so_exit_with_non_zero_val 2>nul
goto endofperl
@rem ';
#!/usr/bin/perl
#line 15
#------------------------------------------------------------------------------
# Project  : Oracle to Postgresql converter
# Name     : ora2pg
# Author   : Gilles Darold, gilles _AT_ darold _DOT_ net
# Copyright: Copyright (c) 2000-2018 : Gilles Darold - All rights reserved -
# Function : Script used to convert Oracle Database to PostgreSQL
# Usage    : ora2pg configuration_file
#------------------------------------------------------------------------------
#
#        This program is free software: you can redistribute it and/or modify
#        it under the terms of the GNU General Public License as published by
#        the Free Software Foundation, either version 3 of the License, or
#        any later version.
# 
#        This program is distributed in the hope that it will be useful,
#        but WITHOUT ANY WARRANTY; without even the implied warranty of
#        MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#        GNU General Public License for more details.
# 
#        You should have received a copy of the GNU General Public License
#        along with this program. If not, see < http://www.gnu.org/licenses/ >.
# 
#------------------------------------------------------------------------------
use strict qw/vars/;
# use warnings;
# use diagnostics;

use Ora2Pg;
use Getopt::Long qw(:config no_ignore_case bundling);
use File::Spec qw/ tmpdir /;
use POSIX qw(locale_h sys_wait_h _exit);
use File::Path qw(make_path);
setlocale(LC_NUMERIC, '');
setlocale(LC_ALL,     'C');

my $VERSION = '19.0';

$| = 1;

my $CONFIG_FILE = 'C:\ora2pg\ora2pg.conf';
my $FILE_CONF = '';
my $DEBUG = 0;
my $QUIET = 0;
my $HELP = 0;
my $LOGFILE = '';
my $EXPORT_TYPE = '';
my $OUTFILE = '';
my $OUTDIR = '';
my $SHOW_VER = 0;
my $PLSQL = '';
my $DSN = '';
my $DBUSER = '';
my $DBPWD = '';
my $SCHEMA = '';
my $TABLEONLY = '';
my $FORCEOWNER = '';
my $ORA_ENCODING = '';
my $PG_ENCODING = '';
my $INPUT_FILE = '';
my $EXCLUDE = '';
my $ALLOW = '';
my $VIEW_AS_TABLE = '';
my $ESTIMATE_COST;
my $COST_UNIT_VALUE;
my $DUMP_AS_HTML;
my $DUMP_AS_CSV;
my $DUMP_AS_SHEET;
my $THREAD_COUNT;
my $ORACLE_COPIES;
my $PARALLEL_TABLES;
my $DATA_LIMIT;
my $CREATE_PROJECT = '';
my $PROJECT_BASE = '.';
my $PRINT_HEADER = '';
my $HUMAN_DAY_LIMIT;
my $IS_MYSQL = 0;
my $IS_SQLSERVER = 1;
my $AUDIT_USER = '';
my $PG_DSN = '';
my $PG_USER = '';
my $PG_PWD = '';
my $COUNT_ROWS = 0;
my $DATA_TYPE = '';
my $GRANT_OBJECT = '';
my $PG_SCHEMA = '';
my $NO_HEADER = 0;

my @SCHEMA_ARRAY  = qw( TABLE PACKAGE VIEW GRANT SEQUENCE TRIGGER FUNCTION PROCEDURE TABLESPACE PARTITION TYPE MVIEW DBLINK SYNONYM DIRECTORY );
my @EXTERNAL_ARRAY  = qw( KETTLE FDW );
my @REPORT_ARRAY  = qw( SHOW_VERSION SHOW_REPORT SHOW_SCHEMA SHOW_TABLE SHOW_COLUMN SHOW_ENCODING  );
my @TEST_ARRAY  = qw( TEST TEST_VIEW);
my @SOURCES_ARRAY = qw( PACKAGE VIEW TRIGGER FUNCTION PROCEDURE PARTITION TYPE MVIEW );
my @DATA_ARRAY    = qw( INSERT COPY );
my @CAPABILITIES  = qw( QUERY LOAD );

my @MYSQL_SCHEMA_ARRAY  = qw( TABLE VIEW GRANT TRIGGER FUNCTION PROCEDURE PARTITION DBLINK );
my @MYSQL_SOURCES_ARRAY = qw( VIEW TRIGGER FUNCTION PROCEDURE PARTITION );

my @SQLSERVER_SCHEMA_ARRAY  = qw( TABLE VIEW GRANT TRIGGER FUNCTION PROCEDURE PARTITION DBLINK);
my @SQLSERVER_SOURCES_ARRAY = qw( VIEW TRIGGER FUNCTION PROCEDURE PARTITION );

my @GRANT_OBJECTS_ARRAY = ('USER','TABLE','VIEW','MATERIALIZED VIEW','SEQUENCE','PROCEDURE','FUNCTION','PACKAGE BODY','TYPE','SYNONYM','DIRECTORY');

my $TMP_DIR      = File::Spec->tmpdir() || '/tmp';

# Collect command line arguments
GetOptions (
	'a|allow=s' => \$ALLOW,
        'b|basedir=s' => \$OUTDIR,
        'c|conf=s' => \$FILE_CONF,
        'd|debug!' => \$DEBUG,
        'D|data_type=s' => \$DATA_TYPE,
	'e|exclude=s' => \$EXCLUDE,
	'g|grant_object=s' => \$GRANT_OBJECT,
        'h|help!' => \$HELP,
	'i|input_file=s' => \$INPUT_FILE,
	'j|jobs=i' => \$THREAD_COUNT,
	'J|copies=i' => \$ORACLE_COPIES,
        'l|log=s' => \$LOGFILE,
	'L|limit=i' => \$DATA_LIMIT,
	'm|mysql!' => \$IS_MYSQL,
        'o|out=s' => \$OUTFILE,
	'p|plsql!' => \$PLSQL,
	'P|parallel=i' =>\$PARALLEL_TABLES,
	'q|quiet!' => \$QUIET,
        't|type=s' => \$EXPORT_TYPE,
        'T|temp_dir=s' => \$TMP_DIR,
	'v|version!' => \$SHOW_VER,
	's|source=s' => \$DSN,
	'S|sql_server!' => \$IS_SQLSERVER,
	'u|user=s' => \$DBUSER,
	'w|password=s' => \$DBPWD,
	'n|namespace=s' => \$SCHEMA,
	'N|pg_schema=s' => \$PG_SCHEMA,
	'x|xtable=s' => \$TABLEONLY, # Obsolete
	'forceowner=s' => \$FORCEOWNER,
	'nls_lang=s' => \$ORA_ENCODING,
	'client_encoding=s' => \$PG_ENCODING,
	'view_as_table=s' => \$VIEW_AS_TABLE,
	'estimate_cost!' =>\$ESTIMATE_COST,
	'cost_unit_value=i' =>\$COST_UNIT_VALUE,
	'dump_as_html!' =>\$DUMP_AS_HTML,
	'dump_as_csv!' =>\$DUMP_AS_CSV,
	'dump_as_sheet!' =>\$DUMP_AS_SHEET,
	'init_project=s' => \$CREATE_PROJECT,
	'project_base=s' => \$PROJECT_BASE,
	'print_header!' => \$PRINT_HEADER,
	'human_days_limit=i' => \$HUMAN_DAY_LIMIT,
	'audit_user=s' => \$AUDIT_USER,
	'pg_dsn=s' => \$PG_DSN,
	'pg_user=s' => \$PG_USER,
	'pg_pwd=s' => \$PG_PWD,
	'count_rows!' => \$COUNT_ROWS,
	'no_header!' => \$NO_HEADER,
);

# Check command line parameters
if ($SHOW_VER) {
	print "Ora2Pg v$VERSION\n";
	exit 0;
}
if ($HELP) {
	&usage();
}

if ($IS_MYSQL) {
	@SCHEMA_ARRAY = @MYSQL_SCHEMA_ARRAY;
	@SOURCES_ARRAY = @MYSQL_SOURCES_ARRAY;
	@EXTERNAL_ARRAY = ();
}

if ($IS_SQLSERVER) {
	@SCHEMA_ARRAY = @SQLSERVER_SCHEMA_ARRAY;
	@SOURCES_ARRAY = @SQLSERVER_SOURCES_ARRAY;
	@EXTERNAL_ARRAY = ();
}

# Create project repository and useful stuff
if ($CREATE_PROJECT) {
	if (!-d "$PROJECT_BASE") {
		print "FATAL: Project base directory does not exists: $PROJECT_BASE\n";
		&usage();
	}
	print STDERR "Creating project $CREATE_PROJECT.\n";
	&create_project($CREATE_PROJECT, $PROJECT_BASE);
	exit 0;
}

if ($GRANT_OBJECT && !grep(/^$GRANT_OBJECT$/, @GRANT_OBJECTS_ARRAY)) {
	print "FATAL: invalid grant object type in -g option. See GRAND_OBJECT configuration directive.\n";
	exit 1;
}

# Clean temporary files
unless(opendir(DIR, "$TMP_DIR")) {
	print "FATAL: can't opendir $TMP_DIR: $!\n";
	exit 1;
}
my @files = grep { $_ =~ /^tmp_ora2pg.*$/ } readdir(DIR);
closedir DIR;
foreach (@files) {
	if (not unlink("$TMP_DIR/$_\n")){
		print "FATAL: can not remove old temporary files $TMP_DIR/$_\n";
		exit 1;
	}
}

# Check configuration file
my $GOES_WITH_DEFAULT = 0;
if ($FILE_CONF && ! -e $FILE_CONF) {
	print "FATAL: can't find configuration file $FILE_CONF\n";
	&usage();
} elsif (!$FILE_CONF && ! -e $CONFIG_FILE) {
	# At least we need configuration to connect to Oracle
	if (!$DSN || (!$DBUSER && !$ENV{ORA2PG_USER}) || (!$DBPWD && !$ENV{ORA2PG_PASSWD})) {
		print "FATAL: can't find configuration file $CONFIG_FILE\n";
		&usage();
	}
	$CONFIG_FILE = '';
	$GOES_WITH_DEFAULT = 1;
}

push(@CAPABILITIES, @SCHEMA_ARRAY, @REPORT_ARRAY, @DATA_ARRAY, @EXTERNAL_ARRAY, @TEST_ARRAY);

# Validate export type
$EXPORT_TYPE = uc($EXPORT_TYPE);
$EXPORT_TYPE =~ s/DATA/COPY/;
foreach my $t (split(/[,;\s\t]+/, $EXPORT_TYPE)) {
	if ($t && !grep(/^$t$/, @CAPABILITIES)) {
		print "FATAL: Unknow export type: $t. Type supported: ", join(',', @CAPABILITIES), "\n";
		&usage();
	}
}

# Preserve barckward compatibility
if ($TABLEONLY) {
	warn "-x | --xtable is deprecated, use -a | --allow option instead.\n";
	if (!$ALLOW) {
		$ALLOW = $TABLEONLY;
	}
}

sub getout
{
        my $sig = shift;
        print STDERR "Received terminating signal ($sig).\n";
        $SIG{INT} = \&getout;
        $SIG{TERM} = \&getout;

	# Cleaning temporary files
	unless(opendir(DIR, "$TMP_DIR")) {
		print "FATAL: can't opendir $TMP_DIR: $!\n";
		exit 1;
	}
	my @files = grep { $_ =~ /^tmp_ora2pg.*$/ } readdir(DIR);
	closedir DIR;
	foreach (@files) {
		unlink("$TMP_DIR/$_\n");
	}

	exit 1;
}

$SIG{INT} = \&getout;
$SIG{TERM} = \&getout;

# Replace ; or space by comma in the user list
$AUDIT_USER =~ s/[;\s]+/,/g;

# Create an instance of the Ora2Pg perl module
my $schema = new Ora2Pg (
	config => $FILE_CONF || $CONFIG_FILE,
	type   => $EXPORT_TYPE,
	debug  => $DEBUG,
	logfile=> $LOGFILE,
	output => $OUTFILE,
	output_dir => $OUTDIR,
	plsql_pgsql => $PLSQL,
	datasource => $DSN,
        user => $DBUSER || $ENV{ORA2PG_USER},
        password => $DBPWD || $ENV{ORA2PG_PASSWD},
	schema => $SCHEMA,
	pg_schema => $PG_SCHEMA,
	force_owner => $FORCEOWNER,
        nls_lang => $ORA_ENCODING,
        client_encoding => $PG_ENCODING,
        input_file => $INPUT_FILE,
	quiet => $QUIET,
	exclude => $EXCLUDE,
	allow => $ALLOW,
	view_as_table => $VIEW_AS_TABLE,
	estimate_cost => $ESTIMATE_COST,
	cost_unit_value => $COST_UNIT_VALUE,
	dump_as_html => $DUMP_AS_HTML,
	dump_as_csv => $DUMP_AS_CSV,
	dump_as_sheet => $DUMP_AS_SHEET,
	thread_count => $THREAD_COUNT,
	oracle_copies => $ORACLE_COPIES,
	data_limit => $DATA_LIMIT,
	parallel_tables => $PARALLEL_TABLES,
	print_header => $PRINT_HEADER,
	human_days_limit => $HUMAN_DAY_LIMIT,
	is_mysql => $IS_MYSQL,
	is_sqlserver => $IS_SQLSERVER,
	audit_user => $AUDIT_USER,
	temp_dir => $TMP_DIR,
	pg_dsn => $PG_DSN,
	pg_user => $PG_USER,
	pg_pwd => $PG_PWD,
	count_rows => $COUNT_ROWS,
	data_type => $DATA_TYPE,
	grant_object => $GRANT_OBJECT,
	no_header => $NO_HEADER,
);

# Look at configuration file if an input file is defined
if (!$INPUT_FILE && !$GOES_WITH_DEFAULT) {
	my $cf_file = $FILE_CONF || $CONFIG_FILE;
	my $fh = new IO::File;
	$fh->open($cf_file) or die "FATAL: can't read configuration file $cf_file, $!\n";
	while (my $l = <$fh>) {
		chomp($l);
		$l =~ s/\r//gs;
		$l =~ s/^\s*\#.*$//g;
		next if (!$l || ($l =~ /^\s+$/));
		$l =~ s/^\s*//; $l =~ s/\s*$//;
		my ($var, $val) = split(/\s+/, $l, 2);
		$var = uc($var);
		if ($var eq 'INPUT_FILE' && $val) {
			$INPUT_FILE = $val;
		}
	}
	$fh->close();
}

# Proceed to Oracle DB extraction following
# configuration file definitions.
if ( ($EXPORT_TYPE !~ /^SHOW_/i) && !$INPUT_FILE ) {
	$schema->export_schema();
}

# Check if error occurs during data export 
unless(opendir(DIR, "$TMP_DIR")) {
	print "FATAL: can't opendir $TMP_DIR: $!\n";
	exit 1;
}
@files = grep { $_ =~ /^tmp_ora2pg.*$/ } readdir(DIR);
closedir DIR;
if ($#files >= 0) {
	print STDERR "\nWARNING: an error occurs during data export. Please check what's happen.\n\n";
	exit 2;
}

exit(0);

####
# Show usage
####
sub usage
{
	print qq{
Usage: ora2pg [-dhpqv --estimate_cost --dump_as_html] [--option value]

    -a | --allow str  : Comma separated list of objects to allow from export.
			Can be used with SHOW_COLUMN too.
    -b | --basedir dir: Set the default output directory, where files
			resulting from exports will be stored.
    -c | --conf file  : Set an alternate configuration file other than the
			default /etc/ora2pg/ora2pg.conf.
    -d | --debug      : Enable verbose output.
    -D | --data_type STR : Allow custom type replacement at command line.
    -e | --exclude str: Comma separated list of objects to exclude from export.
			Can be used with SHOW_COLUMN too.
    -h | --help       : Print this short help.
    -g | --grant_object type : Extract privilege from the given object type.
			See possible values with GRANT_OBJECT configuration.
    -i | --input file : File containing Oracle PL/SQL code to convert with
			no Oracle database connection initiated.
    -j | --jobs num   : Number of parallel process to send data to PostgreSQL.
    -J | --copies num : number of parallel connection to extract data from Oracle.
    -l | --log file   : Set a log file. Default is stdout.
    -L | --limit num  : Number of tuples extracted from Oracle and stored in
			memory before writing, default: 10000.
    -m | --mysql      : Export a MySQL database instead of an Oracle schema.
    -n | --namespace schema : Set the Oracle schema to extract from.
    -N | --pg_schema schema : Set PostgreSQL's search_path.
    -o | --out file   : Set the path to the output file where SQL will
			be written. Default: output.sql in running directory.
    -p | --plsql      : Enable PLSQL to PLPGSQL code conversion.
    -P | --parallel num: Number of parallel tables to extract at the same time.
    -q | --quiet      : Disable progress bar.
    -s | --source DSN : Allow to set the Oracle DBI datasource.
    -S | --sqlserver  : Export a SQL Server database instead of an Oracle schema.  
    -t | --type export: Set the export type. It will override the one
			given in the configuration file (TYPE).
    -T | --temp_dir DIR: Set a distinct temporary directory when two
                         or more ora2pg are run in parallel.
    -u | --user name  : Set the Oracle database connection user.
		        ORA2PG_USER environment variable can be used instead.
    -v | --version    : Show Ora2Pg Version and exit.
    -w | --password pwd : Set the password of the Oracle database user.
		        ORA2PG_PASSWD environment variable can be used instead.
    --forceowner      : Force ora2pg to set tables and sequences owner like in
		  Oracle database. If the value is set to a username this one
		  will be used as the objects owner. By default it's the user
		  used to connect to the Pg database that will be the owner.
    --nls_lang code: Set the Oracle NLS_LANG client encoding.
    --client_encoding code: Set the PostgreSQL client encoding.
    --view_as_table str: Comma separated list of view to export as table.
    --estimate_cost   : Activate the migration cost evalution with SHOW_REPORT
    --cost_unit_value minutes: Number of minutes for a cost evalution unit.
		  default: 5 minutes, correspond to a migration conducted by a
		  PostgreSQL expert. Set it to 10 if this is your first migration.
   --dump_as_html     : Force ora2pg to dump report in HTML, used only with
                        SHOW_REPORT. Default is to dump report as simple text.
   --dump_as_csv      : As above but force ora2pg to dump report in CSV.
   --dump_as_sheet    : Report migration assessment one CSV line per database.
   --init_project NAME: Initialise a typical ora2pg project tree. Top directory
                        will be created under project base dir.
   --project_base DIR : Define the base dir for ora2pg project trees. Default
                        is current directory.
   --print_header     : Used with --dump_as_sheet to print the CSV header
                        especially for the first run of ora2pg.
   --human_days_limit num : Set the number human-days limit where the migration
                        assessment level switch from B to C. Default is set to
                        5 human-days.
   --audit_user LIST  : Comma separated list of username to filter queries in
                        the DBA_AUDIT_TRAIL table. Used only with SHOW_REPORT
                        and QUERY export type.
   --pg_dsn DSN       : Set the datasource to PostgreSQL for direct import.
   --pg_user name     : Set the PostgreSQL user to use.
   --pg_pwd password  : Set the PostgreSQL password to use.
   --count_rows       : Force ora2pg to perform a real row count in TEST action.
   --no_header        : Do not append Ora2Pg header to output file

See full documentation at http://ora2pg.darold.net/ for more help or see
manpage with 'man ora2pg'.

ora2pg will return 0 on success, 1 on error. It will return 2 when a child
process has been interrupted and you've gotten the warning message:
    "WARNING: an error occurs during data export. Please check what's happen."
Most of the time this is an OOM issue, first try reducing DATA_LIMIT value.

};
	exit 1;

}

####
# Create a generic project tree
####
sub create_project
{
	my ($create_project, $project_base) = @_;

	# Look at default configuration file to use
	my $conf_file = $CONFIG_FILE . '.dist';
	if ($FILE_CONF) {
		# Use file given in parameter
		$conf_file = $FILE_CONF;
	}
	if (!-f $conf_file || -z $conf_file) {
		print "FATAL: file $conf_file does not exists.\n";
		exit 1;
	}
	
	# Build entire project tree
	my $base_path = $create_project;
	if (-e $base_path) {
		print "FATAL: project directory exists $base_path\n";
		exit 1;
	}
	make_path ("$base_path");
	print "$base_path/\n";
	make_path ("$base_path/schema");
	print "\tschema/\n";

	foreach my $exp (sort @SCHEMA_ARRAY ) {
		my $tpath = lc($exp);
		$tpath =~ s/y$/ie/;
		mkdir("$base_path/schema/" . $tpath . 's');
		print "\t\t" . $tpath . "s/\n";
	}
	mkdir("$base_path/sources");
	print "\tsources/\n";
	foreach my $exp (sort @SOURCES_ARRAY ) {
		my $tpath = lc($exp);
		$tpath =~ s/y$/ie/;
		mkdir("$base_path/sources/" . $tpath . 's');
		print "\t\t" . $tpath . "s/\n";
	}
	mkdir("$base_path/data");
	print "\tdata/\n";
	mkdir("$base_path/config");
	print "\tconfig/\n";
	mkdir("$base_path/reports");
	print "\treports/\n";
	print "\n";

	# Copy configuration file and transform it as a generic one
	print "Generating generic configuration file\n";
	if (open(IN, "$conf_file")) {
		my @cf = <IN>;
		close(IN);
		# Create a generic configuration file only if it has the .dist extension
		# otherwise use the configuration given at command line (-c option) 
		if ($conf_file =~ /\.dist/) {
			&make_config_generic(\@cf);
		}
		unless(open(OUT, ">$base_path/config/ora2pg.conf")) {
			print "FATAL: can't write to file $base_path/config/ora2pg.conf\n";
			exit 1;
		}
		print OUT @cf;
		close(OUT);
	} else {
		print "FATAL: can not read file $conf_file, $!.\n";
		exit 1;
	}

	# Generate shell script to execute all export
	print "Creating script export_schema.sh to automate all exports.\n";
	unless(open(OUT, "> $base_path/export_schema.sh")) {
		print "FATAL: Can't write to file $base_path/export_schema.sh\n";
		exit 1;
	}
	print OUT qq{#!/bin/sh
#-------------------------------------------------------------------------------
#
# Generated by Ora2Pg, the Oracle database Schema converter, version $VERSION
#
#-------------------------------------------------------------------------------
};
	print OUT "EXPORT_TYPE=\"", join(' ', @SCHEMA_ARRAY), "\"\n";
	print OUT "SOURCE_TYPE=\"", join(' ', @SOURCES_ARRAY), "\"\n";
	print OUT "namespace=\".\"\n";
	print OUT qq{
ora2pg -t SHOW_TABLE -c \$namespace/config/ora2pg.conf > \$namespace/reports/tables.txt
ora2pg -t SHOW_COLUMN -c \$namespace/config/ora2pg.conf > \$namespace/reports/columns.txt
ora2pg -t SHOW_REPORT -c \$namespace/config/ora2pg.conf --dump_as_html --estimate_cost > \$namespace/reports/report.html

for etype in \$(echo \$EXPORT_TYPE | tr " " "\\n")
do
        ltype=`echo \$etype | tr '[:upper:]' '[:lower:]'`
        ltype=`echo \$ltype | sed 's/y\$/ie/'`
        echo "Running: ora2pg -p -t \$etype -o \$ltype.sql -b \$namespace/schema/\$\{ltype\}s -c \$namespace/config/ora2pg.conf"
        ora2pg -p -t \$etype -o \$ltype.sql -b \$namespace/schema/\$\{ltype\}s -c \$namespace/config/ora2pg.conf
	ret=`grep "Nothing found" \$namespace/schema/\$\{ltype\}s/\$ltype.sql 2> /dev/null`
	if [ ! -z "\$ret" ]; then
		rm \$namespace/schema/\$\{ltype\}s/\$ltype.sql
	fi
done

for etype in \$(echo \$SOURCE_TYPE | tr " " "\\n")
do
        ltype=`echo \$etype | tr '[:upper:]' '[:lower:]'`
        ltype=`echo \$ltype | sed 's/y\$/ie/'`
        echo "Running: ora2pg -t \$etype -o \$ltype.sql -b \$namespace/sources/\$\{ltype\}s -c \$namespace/config/ora2pg.conf"
        ora2pg -t \$etype -o \$ltype.sql -b \$namespace/sources/\$\{ltype\}s -c \$namespace/config/ora2pg.conf
	ret=`grep "Nothing found" \$namespace/sources/\$\{ltype\}s/\$ltype.sql 2> /dev/null`
	if [ ! -z "\$ret" ]; then
		rm \$namespace/sources/\$\{ltype\}s/\$ltype.sql
	fi
done

echo
echo
echo "To extract data use the following command:"
echo
echo "ora2pg -t COPY -o data.sql -b \$namespace/data -c \$namespace/config/ora2pg.conf"
echo

exit 0
};
	close(OUT);
	chmod(0700, "$base_path/export_schema.sh");


	# Generate shell script to execute all import
	print "Creating script import_all.sh to automate all imports.\n";
	my $exportype = "EXPORT_TYPE=\"TYPE " . join(' ', grep( !/^TYPE$/, @SCHEMA_ARRAY)) . "\"\n";
	unless(open(OUT, "> $base_path/import_all.sh")) {
		print "FATAL: Can't write to file $base_path/import_all.sh\n";
		exit 1;
	}
	while (my $l = <DATA>) {
		$l =~ s/^EXPORT_TYPE=.*/$exportype/s;
		$l =~ s/ORA2PG_VERSION/$VERSION/s;
		print OUT $l;
	}
	close(OUT);
	chmod(0700, "$base_path/import_all.sh");
}

####
# Set a generic configuration
####
sub make_config_generic
{
	my $conf_arr = shift;

	chomp(@$conf_arr);

	my $schema = 'CHANGE_THIS_SCHEMA_NAME';
	$schema = $SCHEMA if ($SCHEMA);
	for (my $i = 0; $i <= $#{$conf_arr}; $i++) {
		if ($IS_MYSQL) {
			$conf_arr->[$i] =~ s/^# Set Oracle database/# Set MySQL database/;
			$conf_arr->[$i] =~ s/^(ORACLE_DSN.*dbi):Oracle:(.*);sid=SIDNAME/$1:mysql:$2;database=dbname/;
			$conf_arr->[$i] =~ s/CHANGE_THIS_SCHEMA_NAME/CHANGE_THIS_DB_NAME/;
			$conf_arr->[$i] =~ s/#REPLACE_ZERO_DATE.*/REPLACE_ZERO_DATE\t-INFINITY/;
		}
		elsif ($IS_SQLSERVER) {
			$conf_arr->[$i] =~ s/^# Set Oracle database/# Set SQL Server database/;
			$conf_arr->[$i] =~ s/^(ORACLE_DSN.*dbi):Oracle:(.*);sid=SIDNAME/$1:SQLSERVER:$2;database=dbname/; 
			# ->connect("dbi:ODBC:Driver={ODBC Driver 17 for SQL Server};Server=$server;UID=$user;PWD=$password")
			# $conf_arr->[$i] =~ s/#REPLACE_ZERO_DATE.*/REPLACE_ZERO_DATE\t-INFINITY/;
		}
		elsif ($ENV{ORACLE_HOME}) {
			$conf_arr->[$i] =~ s/^ORACLE_HOME.*/ORACLE_HOME\t$ENV{ORACLE_HOME}/;
		}
		$conf_arr->[$i] =~ s/^USER_GRANTS.*0/USER_GRANTS\t1/;
		$conf_arr->[$i] =~ s/^#SCHEMA.*SCHEMA_NAME/SCHEMA\t$schema/;
		$conf_arr->[$i] =~ s/^(BINMODE.*)/#$1/;
		$conf_arr->[$i] =~ s/^PLSQL_PGSQL.*1/PLSQL_PGSQL\t0/;
		$conf_arr->[$i] =~ s/^FILE_PER_CONSTRAINT.*0/FILE_PER_CONSTRAINT\t1/;
		$conf_arr->[$i] =~ s/^FILE_PER_INDEX.*0/FILE_PER_INDEX\t1/;
		$conf_arr->[$i] =~ s/^FILE_PER_FKEYS.*0/FILE_PER_FKEYS\t1/;
		$conf_arr->[$i] =~ s/^FILE_PER_TABLE.*0/FILE_PER_TABLE\t1/;
		$conf_arr->[$i] =~ s/^FILE_PER_SEQUENCE.*0/FILE_PER_SEQUENCE\t1/;
		$conf_arr->[$i] =~ s/^FILE_PER_FUNCTION.*0/FILE_PER_FUNCTION\t1/;
		$conf_arr->[$i] =~ s/^TRUNCATE_TABLE.*0/TRUNCATE_TABLE\t1/;
		$conf_arr->[$i] =~ s/^DISABLE_SEQUENCE.*0/DISABLE_SEQUENCE\t1/;
		$conf_arr->[$i] =~ s/^DISABLE_TRIGGERS.*0/DISABLE_TRIGGERS\t1/;
		$conf_arr->[$i] =~ s/^(CLIENT_ENCODING.*)/#$1/;
		$conf_arr->[$i] =~ s/^(NLS_LANG.*)/#$1/;
		$conf_arr->[$i] =~ s/^#LONGREADLEN.*1047552/LONGREADLEN\t1047552/;
		$conf_arr->[$i] =~ s/^AUTODETECT_SPATIAL_TYPE.*0/AUTODETECT_SPATIAL_TYPE\t1/;
		$conf_arr->[$i] =~ s/^NO_LOB_LOCATOR.*/NO_LOB_LOCATOR\t0/;
		$conf_arr->[$i] =~ s/^FTS_INDEX_ONLY.*0/FTS_INDEX_ONLY\t1/;
		$conf_arr->[$i] =~ s/^DISABLE_UNLOGGED.*0/DISABLE_UNLOGGED\t1/;
		if ($DSN) {
			$conf_arr->[$i] =~ s/^ORACLE_DSN.*/ORACLE_DSN\t$DSN/;
		}
		if ($DBUSER) {
			$conf_arr->[$i] =~ s/^ORACLE_USER.*/ORACLE_USER\t$DBUSER/;
		}
		if ($DBPWD) {
			$conf_arr->[$i] =~ s/^ORACLE_PWD.*/ORACLE_PWD\t$DBPWD/;
		}
	}
	map { s/$/\n/; } @$conf_arr;
}

__DATA__
#!/bin/sh
#-------------------------------------------------------------------------------
#
# Script used to load exported sql files into PostgreSQL in practical manner
# allowing you to chain and automatically import schema and data.
#
# Generated by Ora2Pg, the Oracle database Schema converter, version ORA2PG_VERSION
#
#-------------------------------------------------------------------------------

EXPORT_TYPE="TYPE,TABLE,PARTITION,VIEW,MVIEW,FUNCTION,PROCEDURE,SEQUENCE,TRIGGER,SYNONYM,DIRECTORY,DBLINK"
AUTORUN=0
NAMESPACE=.
NO_CONSTRAINTS=0
IMPORT_INDEXES_AFTER=0
DEBUG=0
SCHEMA_ONLY=0
DATA_ONLY=0
CONSTRAINTS_ONLY=0
NO_DBCHECK=0


# Message functions
die() {
    echo "ERROR: $1" 1>&2
    exit 1
}

usage() {
    echo "usage: `basename $0` [options]"
    echo ""
    echo "Script used to load exported sql files into PostgreSQL in practical manner"
    echo "allowing you to chain and automatically import schema and data."
    echo ""
    echo "options:"
    echo "    -a             import data only"
    echo "    -b filename    SQL script to execute just after table creation to fix database schema"
    echo "    -d dbname      database name for import"
    echo "    -D             enable debug mode, will only show what will be done"
    echo "    -e encoding    database encoding to use at creation (default: UTF8)"
    echo "    -f             force no check of user and database existing and do not try to create them"
    echo "    -h hostname    hostname of the PostgreSQL server (default: unix socket)"
    echo "    -i             only load indexes, constraints and triggers"
    echo "    -I             do not try to load indexes, constraints and triggers"
    echo "    -j cores       number of connection to use to import data or indexes into PostgreSQL"
    echo "    -l filename    log file where stdout+stderr are redirected (default: stdout)"
    echo "    -n schema      comma separated list of schema to create"
    echo "    -o username    owner of the database to create"
    echo "    -p port        listening port of the PostgreSQL server (default: 5432)"
    echo "    -P cores       number of tables to process at same time for data import"
    echo "    -s             import schema only, do not try to import data"
    echo "    -t export      comma separated list of export type to import (same as ora2pg)"
    echo "    -U username    username to connect to PostgreSQL (default: peer username)"
    echo "    -x             import indexes and constraints after data"
    echo "    -y             reply Yes to all questions for automatic import"
    echo
    echo "    -?             print help"
    echo
    exit $1
}

# Function to emulate Perl prompt function
confirm () {

    msg=$1
    if [ "$AUTORUN" != "0" ]; then
	true
    else
	    if [ -z "$msg" ]; then
		msg="Are you sure? [y/N/q]"
	    fi
	    # call with a prompt string or use a default
	    read -r -p "${msg} [y/N/q] " response
	    case $response in
		[yY][eE][sS]|[yY]) 
		    true
		    ;;
		[qQ][uU][iI][tT]|[qQ]) 
		    exit
		    ;;
		*)
		    false
		    ;;
	    esac
    fi
}

# Function used to import constraints and indexes
import_constraints () {
	if [ -r "$NAMESPACE/schema/tables/INDEXES_table.sql" ]; then
		if confirm "Would you like to import indexes from $NAMESPACE/schema/tables/INDEXES_table.sql?" ; then
			if [ -z "$IMPORT_JOBS" ]; then
				echo "Running: psql$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/schema/tables/INDEXES_table.sql"
				if [ $DEBUG -eq 0 ]; then
					psql$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/schema/tables/INDEXES_table.sql
					if [ $? -ne 0 ]; then
						die "can not import indexes."
					fi
				fi
			else
				echo "Running: ora2pg -c config/ora2pg.conf -t LOAD -i $NAMESPACE/schema/tables/INDEXES_table.sql"
				if [ $DEBUG -eq 0 ]; then
					ora2pg$IMPORT_JOBS -c config/ora2pg.conf -t LOAD -i $NAMESPACE/schema/tables/INDEXES_table.sql
					if [ $? -ne 0 ]; then
						die "can not import indexes."
					fi
				fi
			fi
		fi
	fi

	if [ -r "$NAMESPACE/schema/tables/CONSTRAINTS_table.sql" ]; then
		if confirm "Would you like to import constraints from $NAMESPACE/schema/tables/CONSTRAINTS_table.sql?" ; then
			if [ -z "$IMPORT_JOBS" ]; then
				echo "Running: psql$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/schema/tables/CONSTRAINTS_table.sql"
				if [ $DEBUG -eq 0 ]; then
					psql$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/schema/tables/CONSTRAINTS_table.sql
					if [ $? -ne 0 ]; then
						die "can not import constraints."
					fi
				fi
			else
				echo "Running: ora2pg$IMPORT_JOBS -c config/ora2pg.conf -t LOAD -i $NAMESPACE/schema/tables/CONSTRAINTS_table.sql"
				if [ $DEBUG -eq 0 ]; then
					ora2pg$IMPORT_JOBS -c config/ora2pg.conf -t LOAD -i $NAMESPACE/schema/tables/CONSTRAINTS_table.sql
					if [ $? -ne 0 ]; then
						die "can not import constraints."
					fi
				fi
			fi
		fi
	fi

	if [ -r "$NAMESPACE/schema/tables/FKEYS_table.sql" ]; then
		if confirm "Would you like to import foreign keys from $NAMESPACE/schema/tables/FKEYS_table.sql?" ; then
			if [ -z "$IMPORT_JOBS" ]; then
				echo "Running: psql$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/schema/tables/FKEYS_table.sql"
				if [ $DEBUG -eq 0 ]; then
					psql$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/schema/tables/FKEYS_table.sql
					if [ $? -ne 0 ]; then
						die "can not import foreign keys."
					fi
				fi
			else
				echo "Running: ora2pg$IMPORT_JOBS -c config/ora2pg.conf -t LOAD -i $NAMESPACE/schema/tables/FKEYS_table.sql"
				if [ $DEBUG -eq 0 ]; then
					ora2pg$IMPORT_JOBS -c config/ora2pg.conf -t LOAD -i $NAMESPACE/schema/tables/FKEYS_table.sql
					if [ $? -ne 0 ]; then
						die "can not import foreign keys."
					fi
				fi
			fi
		fi
	fi

	if [ $NO_CONSTRAINTS -eq 1 ] && [ -r "$NAMESPACE/schema/triggers/trigger.sql" ]; then
		if confirm "Would you like to import TRIGGER from $NAMESPACE/schema/triggers/trigger.sql?" ; then
			echo "Running: psql --single-transaction$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/schema/triggers/trigger.sql"
			if [ $DEBUG -eq 0 ]; then
				psql --single-transaction$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/schema/triggers/trigger.sql
				if [ $? -ne 0 ]; then
					die "an error occurs when importing file $NAMESPACE/schema/triggers/trigger.sql."
				fi
			fi
		fi
	fi
}

# Command line options
while getopts "b:d:e:h:j:l:n:o:p:P:t:U:aDfiIsyx?"  opt; do
    case "$opt" in
	a) DATA_ONLY=1;;
	b) SQL_POST_SCRIPT=$OPTARG;;
        d) DB_NAME=$OPTARG;;
        D) DEBUG=1;;
        e) DB_ENCODING=" -E $OPTARG";;
	f) NO_DBCHECK=1;;
        h) DB_HOST=" -h $OPTARG";;
        i) CONSTRAINTS_ONLY=1;;
        I) NO_CONSTRAINTS=1;;
        j) IMPORT_JOBS=" -j $OPTARG";;
        l) LOGFILE=$OPTARG;;
        n) DB_SCHEMA=$OPTARG;;
        o) DB_OWNER=$OPTARG;;
        p) DB_PORT=" -p $OPTARG";;
        P) PARALLEL_TABLES=" -P $OPTARG";;
        s) SCHEMA_ONLY=1;;
        t) EXPORT_TYPE=$OPTARG;;
        U) DB_USER=" -U $OPTARG";;
	x) IMPORT_INDEXES_AFTER=1;;
        y) AUTORUN=1;;
        "?") usage 1;;
        *) die "Unknown error while processing options";;
    esac
done

# Check if post tables import SQL script is readable
if [ ! -z "$SQL_POST_SCRIPT" ]; then
	if [ ! -r "$SQL_POST_SCRIPT" ]; then
		die "the SQL script $SQL_POST_SCRIPT is not readable."
	fi
fi

# A database name is mandatory
if [ -z "$DB_NAME" ]; then
	die "you must give a PostgreSQL database name (see -d option)."
fi

# A database owner is mandatory
if [ -z "$DB_OWNER" ]; then
	die "you must give a username to be used as owner of database (see -o option)."
fi

# Check if the project directory is readable
if [ ! -r "$NAMESPACE/schema/tables/table.sql" ]; then
	die "project directory '$NAMESPACE' is not valid or is not readable."
fi

# If constraints and indexes files are present propose to import these object
if [ $CONSTRAINTS_ONLY -eq 1 ]; then
	if confirm "Would you like to load indexes, constraints and triggers?" ; then
		import_constraints
	fi
	exit 0
fi

# When a PostgreSQL schema list is provided, create them
if [ $DATA_ONLY -eq 0 ]; then
	if [ $NO_DBCHECK  -eq 0 ]; then
		# Create owner user
    user_exists=`psql -d $DB_NAME$DB_HOST$DB_PORT$DB_USER -Atc "select usename from pg_user where usename='$DB_OWNER';"`
		if [ "a$user_exists" = "a" ]; then
			if confirm "Would you like to create the owner of the database $DB_OWNER?" ; then
				echo "Running: createuser$DB_HOST$DB_PORT$DB_USER --no-superuser --no-createrole --no-createdb $DB_OWNER"
				if [ $DEBUG -eq 0 ]; then
					createuser$DB_HOST$DB_PORT$DB_USER --no-superuser --no-createrole --no-createdb $DB_OWNER
					if [ $? -ne 0 ]; then
						die "can not create user $DB_OWNER."
					fi
				fi
			fi
		else
			echo "Database owner $DB_OWNER already exists, skipping creation."
		fi

		# Create database if required
		if [ "a$DB_ENCODING" = "a" ]; then
			DB_ENCODING=" -E UTF8"
		fi
    db_exists=`psql -d $DB_NAME$DB_HOST$DB_PORT$DB_USER -Atc "select datname from pg_database where datname='$DB_NAME';"`
		if [ "a$db_exists" = "a" ]; then
			if confirm "Would you like to create the database $DB_NAME?" ; then
				echo "Running: createdb$DB_HOST$DB_PORT$DB_USER$DB_ENCODING --owner $DB_OWNER $DB_NAME"
				if [ $DEBUG -eq 0 ]; then
					createdb$DB_HOST$DB_PORT$DB_USER$DB_ENCODING --owner $DB_OWNER $DB_NAME
					if [ $? -ne 0 ]; then
						die "can not create database $DB_NAME."
					fi
				fi
			fi
		else
			if confirm "Would you like to drop the database $DB_NAME before recreate it?" ; then
				echo "Running: dropdb$DB_HOST$DB_PORT$DB_USER $DB_NAME"
				if [ $DEBUG -eq 0 ]; then
					dropdb$DB_HOST$DB_PORT$DB_USER $DB_NAME
					if [ $? -ne 0 ]; then
						die "can not drop database $DB_NAME."
					fi
				fi
				echo "Running: createdb$DB_HOST$DB_PORT$DB_USER$DB_ENCODING --owner $DB_OWNER $DB_NAME"
				if [ $DEBUG -eq 0 ]; then
					createdb$DB_HOST$DB_PORT$DB_USER$DB_ENCODING --owner $DB_OWNER $DB_NAME
					if [ $? -ne 0 ]; then
						die "can not create database $DB_NAME."
					fi
				fi
			fi
		fi
	fi

	# When schema list is provided, create them
	if [ "a$DB_SCHEMA" != "a" ]; then
		nspace_list=''
		for enspace in $(echo $DB_SCHEMA | tr "," "\n")
		do
			lnspace=`echo $enspace | tr '[:upper:]' '[:lower:]'`
			if confirm "Would you like to create schema $lnspace in database $DB_NAME?" ; then
				echo "Running: psql$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -c \"CREATE SCHEMA $lnspace;\""
				if [ $DEBUG -eq 0 ]; then
					psql$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -c "CREATE SCHEMA $lnspace;"
					if [ $? -ne 0 ]; then
						die "can not create schema $DB_SCHEMA."
					fi
				fi
				nspace_list="$nspace_list$lnspace,"
			fi
		done
		# Change search path of the owner
		if [ "a$nspace_list" != "a" ]; then
			if confirm "Would you like to change search_path of the database owner?" ; then
				echo "Running: psql$DB_HOST$DB_PORT$DB_USER -d $DB_NAME -c \"ALTER ROLE $DB_OWNER SET search_path TO ${nspace_list}public;\""
				if [ $DEBUG -eq 0 ]; then
					psql$DB_HOST$DB_PORT$DB_USER -d $DB_NAME -c "ALTER ROLE $DB_OWNER SET search_path TO ${nspace_list}public;"
					if [ $? -ne 0 ]; then
						die "can not change search_path."
					fi
				fi
			fi
		fi
	fi

	# Then import all files from project directory
	for etype in $(echo $EXPORT_TYPE | tr "," "\n")
	do

		if [ $NO_CONSTRAINTS -eq 1 ] && [ $etype = "TRIGGER" ]; then
			continue
		fi

		if [ $etype = "GRANT" ] || [ $etype = "TABLESPACE" ]; then
			continue
		fi

		ltype=`echo $etype | tr '[:upper:]' '[:lower:]'`
		ltype=`echo $ltype | sed 's/y$/ie/'`
		if [ -r "$NAMESPACE/schema/${ltype}s/$ltype.sql" ]; then
			if confirm "Would you like to import $etype from $NAMESPACE/schema/${ltype}s/$ltype.sql?" ; then
				echo "Running: psql --single-transaction $DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/schema/${ltype}s/$ltype.sql"
				if [ $DEBUG -eq 0 ]; then
					psql --single-transaction $DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/schema/${ltype}s/$ltype.sql
					if [ $? -ne 0 ]; then
						die "an error occurs when importing file $NAMESPACE/schema/${ltype}s/$ltype.sql."
					fi
				fi
			fi
		fi
		if [ ! -z "$SQL_POST_SCRIPT" ] && [ $etype = "TABLE" ]; then
			if confirm "Would you like to execute SQL script $SQL_POST_SCRIPT?" ; then
				echo "Running: psql --single-transaction$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $SQL_POST_SCRIPT"
				if [ $DEBUG -eq 0 ]; then
					psql --single-transaction$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $SQL_POST_SCRIPT
					if [ $? -ne 0 ]; then
						die "an error occurs when importing file $SQL_POST_SCRIPT."
					fi
				fi
			fi
		fi
	done

	# If constraints and indexes files are present propose to import these object
	if [ $NO_CONSTRAINTS -eq 0 ] && [ $IMPORT_INDEXES_AFTER -eq 0 ]; then
		if confirm "Would you like to process indexes and constraints before loading data?" ; then
			IMPORT_INDEXES_AFTER=0
			import_constraints
		else
			IMPORT_INDEXES_AFTER=1
		fi
	fi

	# Import objects that need superuser priviledge: GRANT and TABLESPACE
	if [ -r "$NAMESPACE/schema/grants/grant.sql" ]; then
		if confirm "Would you like to import GRANT from $NAMESPACE/schema/grants/grant.sql?" ; then
			echo "Running: psql $DB_HOST$DB_PORT -U postgres -d $DB_NAME -f $NAMESPACE/schema/grants/grant.sql"
			if [ $DEBUG -eq 0 ]; then
				psql $DB_HOST$DB_PORT -U postgres -d $DB_NAME -f $NAMESPACE/schema/grants/grant.sql
				if [ $? -ne 0 ]; then
					die "an error occurs when importing file $NAMESPACE/schema/grants/grant.sql."
				fi
			fi
		fi
	fi
	if [ -r "$NAMESPACE/schema/tablespaces/tablespace.sql" ]; then
		if confirm "Would you like to import TABLESPACE from $NAMESPACE/schema/tablespaces/tablespace.sql?" ; then
			echo "Running: psql $DB_HOST$DB_PORT -U postgres -d $DB_NAME -f $NAMESPACE/schema/tablespaces/tablespace.sql"
			if [ $DEBUG -eq 0 ]; then
				psql $DB_HOST$DB_PORT -U postgres -d $DB_NAME -f $NAMESPACE/schema/tablespaces/tablespace.sql
				if [ $? -ne 0 ]; then
					die "an error occurs when importing file $NAMESPACE/schema/tablespaces/tablespace.sql."
				fi
			fi
		fi
	fi
fi


# Check if we must just import schema or proceed to data import too
if [ $SCHEMA_ONLY -eq 0 ]; then
	# set the PostgreSQL datasource
	pgdsn_defined=`grep "^PG_DSN" config/ora2pg.conf | sed 's/.*dbi:Pg/dbi:Pg/'`
	if [ "a$pgdsn_defined" = "a" ]; then
		if [ "a$DB_HOST" != "a" ]; then
			pgdsn_defined="dbi:Pg:dbname=$DB_NAME;host=$DB_HOST"
		else
      #default to unix socket
      pgdsn_defined="dbi:Pg:dbname=$DB_NAME;"
    fi
		if [ "a$DB_PORT" != "a" ]; then
			pgdsn_defined="$pgdsn_defined;port=$DB_PORT"
		else
			pgdsn_defined="$pgdsn_defined;port=5432"
		fi
	fi

	# remove command line option from the DSN string
	pgdsn_defined=`echo "$pgdsn_defined" | sed 's/ -. //g'`

	# If data file is present propose to import data
	if [ -r "$NAMESPACE/data/data.sql" ]; then
		if confirm "Would you like to import data from $NAMESPACE/data/data.sql?" ; then
			echo "Running: psql$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/data/data.sql"
			if [ $DEBUG -eq 0 ]; then
				psql$DB_HOST$DB_PORT -U $DB_OWNER -d $DB_NAME -f $NAMESPACE/data/data.sql
				if [ $? -ne 0 ]; then
					die "an error occurs when importing file $NAMESPACE/data/data.sql."
				fi
			fi
		fi
	else
		# Import data directly from PostgreSQL
		if confirm "Would you like to import data from Oracle database directly into PostgreSQL?" ; then
			echo "Running: ora2pg$IMPORT_JOBS$PARALLEL_TABLES -c config/ora2pg.conf -t COPY --pg_dsn \"$pgdsn_defined\" --pg_user $DB_OWNER"
			if [ $DEBUG -eq 0 ]; then
				ora2pg$IMPORT_JOBS$PARALLEL_TABLES -c config/ora2pg.conf -t COPY --pg_dsn "$pgdsn_defined" --pg_user $DB_OWNER
				if [ $? -ne 0 ]; then
					die "an error occurs when importing data."
				fi
			fi
		fi
	fi

	if [ $NO_CONSTRAINTS -eq 0 ] && [ $DATA_ONLY -eq 0 ]; then
		# Import indexes and constraint after data
		if [ $IMPORT_INDEXES_AFTER -eq 1 ]; then
			import_constraints
		fi
	fi
fi

exit 0


__END__
:endofperl
