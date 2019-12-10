package Ora2Pg::SQLServer;

use vars qw($VERSION);
use strict;
use Data::Dumper;
use POSIX qw(locale_h);
$Data::Dumper::Indent = 1;
#set locale to LC_NUMERIC C
setlocale(LC_NUMERIC,"C");

$VERSION = '19.0';

# Some function might be excluded from export and assessment.
our @EXCLUDED_FUNCTION = ();

# These definitions can be overriden from configuration file
# Taken from https://docs.microsoft.com/fr-fr/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-2017
our %SQLSERVER_TYPE = (
	'BIGINT' => 'bigint', 
	'BINARY' => 'bytea',
	'BIT' => 'bit varying',
	'CHAR' => 'char',
	'DATE' => 'date',
	'DATETIME' => 'timestamp without time zone',
	'DATETIME2' => 'timestamp without time zone',
	'DATETIMEOFFSET' => 'timestamp with time zone',
	'DECIMAL' => 'numeric',
	'DOUBLE' => 'double precision',
	'FLOAT' => 'double precision',
	'GEOGRAPHY' => 'bytea', # or use postgis ?
	'HIERARCHYID' => 'bytea', # or ltree ?
	'IMAGE' => 'bytea',
	'INT' => 'integer',
	'MONEY' => 'numeric',
	'NCHAR' => 'char',
	'NUMERIC' => 'numeric',
	'NVARCHAR' => 'varchar',
	'REAL' => 'real',
	'SMALLDATETIME' => 'timestamp without time zone',
	'SMALLINT' => 'smallint',
	'SMALLMONEY' => 'numeric(6,4)',
	'TEXT' => 'text',
	'TIME' => 'time without time zone', 
	'TINYINT' => 'smallint',
	'UNIQUEIDENTIFIER' => 'uuid', # needs uuid-ossp extension for operators
	'VARBINARY' => 'bytea',
	'VARCHAR' => 'varchar',
	'XML' => 'xml',

	# 'NTEXT' => 
	# cursor
	# rowversion
	# sql_variant
	# Spatial Geometry Types
	# Spatial Geography Types
	# table
	
);


#sql date formats  refence
#https://www.sqlshack.com/sql-convert-date-functions-and-formats/
#https://www.mssqltips.com/sqlservertip/1145/date-and-time-conversions-using-sql-server/
	
our %SQL_Date_format = (
	'1' => 'mm/dd/yy', 
	'2' => 'yy.mm.dd',
	'3' => 'dd/mm/yy',
	'4' => 'dd.mm.yy',
	'5' => 'dd-mm-yy',
	'6' => 'dd Mon yy',
	'7' => 'Mon dd,y',
	'8' => 'HH12:MI:SS',
	'10' => 'mm-dd-yy',
	'11' => 'yy/mm/dd',
	'12' => 'yymmdd',
	'13' => 'DD Mon YYYY HH:MI:SS:MS',
	'14' => 'HH:MI:SS:MS',
	'20' => 'yyy-mm-dd HH:MI:SS',
	'21' => 'YYYY-MM-DD HH:MI:SS.MS',
	'22' => 'mm/dd/yy HH:MI:SS AM',
	'23' => 'yyyy-mm-dd',
	'24' => 'HH12:MI:SS',
	'27' => 'mm-dd-yyyy hh:mi:ss.ms',
	'100' => 'Mon dd yyyy HH:SSAM',
	'101' => 'mm/dd/yyyy', # or ltree ?
	'102' => 'yyyy.mm.dd',
	'103' => 'dd/mm/yyyy',
	'104' => 'dd.mm.yyyy',
	'105' => 'dd-mm-yyyy',
	'106' => 'dd Mon yyyy',
	'107' => 'Mon dd,yyyy',
	'108' => 'HH:MI:SS',
	'109' => 'Mon DD YYYY hh:mi:ss:ms',
	'110' => 'MM- DD-YY',
	'111' => 'yyyy/mm/dd',
	'112' => 'YYYYMMDD',
	'113' => 'DD Mon YYYY HH:mi:SS:MS',
	'114' => 'hh:mi:ss:ms',
	'120' => 'YYYY-MM-DD HH:mi:SS',
	'121' => 'YYYY-MM-DD HH:mi:SS.ms',
	'126' => 'YYYY-MM-DDT HH:mi:SS.ms'

);
=head2 _get_version

Pretty print SQL Server version number.

=cut

sub _get_version
{
	my $self = shift;

	my $sqlserver_ver = '';
	my $sql = "SELECT 'SQL Server ' + cast(SERVERPROPERTY('Edition') as varchar) + ' - Version number: ' + cast(SERVERPROPERTY('ProductVersion') as varchar)";
	$sql = 'SELECT @@version';

        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		# $oraver = $row[0];
		$sqlserver_ver = (split /\n/, $row[0])[0];
		last;
	}
	$sth->finish();

	return $sqlserver_ver;
}

=head2 _schema_list

This function returns all SCHEMA names listed under given database  

=cut

sub _schema_list
{
	my $self = shift;

	# my $sql = "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')";
	my $sql = "select schema_name from INFORMATION_SCHEMA.SCHEMATA where SCHEMA_OWNER = 'dbo'";
	my $sth = $self->{dbh}->prepare( $sql ) or return undef;
	$sth->execute or return undef;
	$sth;
}

=head2 _get_encoding

This function retrieves the SQL Server database encoding

Returns a handle to a DB query statement.

=cut

sub _get_encoding
{
	my ($self, $dbh) = @_;

	# my $sql = "SHOW VARIABLES LIKE 'character\\_set\\_%';";
        # my $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
        # $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	# my $my_encoding = '';
	# my $my_client_encoding = '';
	# while ( my @row = $sth->fetchrow()) {
	# 	if ($row[0] eq 'character_set_database') {
	# 		$my_encoding = $row[1];
	# 	} elsif ($row[0] eq 'character_set_client') {
	# 		$my_client_encoding = $row[1];
	# 	}
	# }
	# $sth->finish();

	# my $my_timestamp_format = '';
	# my $my_date_format = '';
	# $sql = "SHOW VARIABLES LIKE '%\\_format';";
        # $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
        # $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	# while ( my @row = $sth->fetchrow()) {
	# 	if ($row[0] eq 'datetime_format') {
	# 		$my_timestamp_format = $row[1];
	# 	} elsif ($row[0] eq 'date_format') {
	# 		$my_date_format = $row[1];
	# 	}
	# }
	# $sth->finish();

	my $my_date_format = '';
	my $sql = 'SELECT dateformat FROM sys.syslanguages WHERE name = @@LANGUAGE';
        my $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
        $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		$my_date_format = $row[0];
		last;
	}
	$sth->finish();

	my $my_encoding = '';

	my $my_client_encoding = '';
	# while ( my @row = $sth->fetchrow()) {
	# 	if ($row[0] eq 'character_set_database') {
	# 		$my_encoding = $row[1];
	# 	} elsif ($row[0] eq 'character_set_client') {
	# 		$my_client_encoding = $row[1];
	# 	}
	# }
	# $sth->finish();

	my $my_timestamp_format = '';


        # $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
        # $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	# while ( my @row = $sth->fetchrow()) {
	# 	if ($row[0] eq 'datetime_format') {
	# 		$my_timestamp_format = $row[1];
	# 	} elsif ($row[0] eq 'date_format') {
	# 		$my_date_format = $row[1];
	# 	}
	# }
	# $sth->finish();



	my $pg_encoding = $my_encoding;

	return ($my_encoding, $my_client_encoding, $pg_encoding, $my_timestamp_format, $my_date_format);
}

=head2 _get_database_size

This function retrieves the size of the SQL Server database in MB

=cut

sub _get_database_size
{
	my $self = shift;

	my $mb_size = '';
	my $condition = '';

# my $sql = qq{
# SELECT TABLE_SCHEMA "DB Name",
#    sum(DATA_LENGTH + INDEX_LENGTH)/1024/1024 "DB Size in MB"
# FROM INFORMATION_SCHEMA.TABLES
# WHERE TABLE_SCHEMA='$self->{schema}'
# GROUP BY TABLE_SCHEMA
# };
	my $sql = qq{
SELECT total_size_mb = CAST(SUM(size) * 8. / 1024 AS DECIMAL(8,2)) 
FROM sys.master_files WITH(NOWAIT) 
WHERE database_id = DB_ID()
};
        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$mb_size = sprintf("%.2f MB", $row[0]);
		last;
	}
	$sth->finish();

	return $mb_size;
}

=head2 _get_objects

This function retrieves all object from SQL Server

=cut

sub _get_objects
{
	my $self = shift;

	my %infos = ();
	# print "GO:1\n";

	# TABLE
	my $sql = "SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'";
	# my $sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG = '$self->{schema}'";
	# print "GO:sql=[$sql]\n";
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	# print "GO:2\n";
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	# print "GO:3\n";
	while ( my @row = $sth->fetchrow()) {
		# print "GO:T\n";
		push(@{$infos{TABLE}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();

	# VIEW
	# $sql = "SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM $self->{schema}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_CATALOG = '$self->{schema}'";
	$sql = "SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS";
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{VIEW}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	
	# PROCEDURE
	# $sql = "SELECT ROUTINE_SCHEMA + '.' + ROUTINE_NAME FROM $self->{schema}.INFORMATION_SCHEMA.ROUTINES where ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_CATALOG = '$self->{schema}'"; # TODO: system procedure also listed
	$sql = "SELECT ROUTINE_SCHEMA + '.' + ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES where ROUTINE_TYPE = 'PROCEDURE'"; # TODO: system procedure also listed
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{PROCEDURE}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();

	# FUNCTION
	# $sql = "SELECT ROUTINE_SCHEMA + '.' + ROUTINE_NAME FROM $self->{schema}.INFORMATION_SCHEMA.ROUTINES where ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_CATALOG = '$self->{schema}'"; # TODO: system procedure also listed
	$sql = "SELECT ROUTINE_SCHEMA + '.' + ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES where ROUTINE_TYPE = 'FUNCTION'"; # TODO: system procedure also listed
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{FUNCTION}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();

	# INDEX
	$sql = "SELECT si.name AS IndexName FROM sys.indexes si JOIN sys.objects so ON si.object_id = so.object_id WHERE so.type = 'U' AND si.name IS NOT NULL";
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{INDEX}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	
	# TRIGGER
	# $sql = qq{SELECT sysobjects.name AS TRIGGER_NAME FROM $self->{schema}.sysobjects INNER JOIN $self->{schema}.sysusers ON sysobjects.uid = sysusers.uid 
	#   INNER JOIN $self->{schema}.sys.tables t ON sysobjects.parent_obj = t.object_id 
	#   INNER JOIN $self->{schema}.sys.schemas s ON t.schema_id = s.schema_id 
	#   WHERE sysobjects.type = 'TR' };
	$sql = "SELECT TR.name FROM sys.triggers AS TR";
	# if ($self->{schema}) {
	# 	$sql .= " and s.name = '$self->{schema}'";
	# }

	# print "TRIG:sql=[$sql]\n";
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{TRIGGER}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	
	# PARTITION.
	#    $sql = qq{
	#    SELECT TABLE_NAME||'_'||PARTITION_NAME
	#    FROM INFORMATION_SCHEMA.PARTITIONS
	#    WHERE SUBPARTITION_NAME IS NULL AND (PARTITION_METHOD = 'RANGE' OR PARTITION_METHOD = 'LIST')
	#    };
	$sql = qq{select object_schema_name(i.object_id) + '.' + object_name(i.object_id) as [object]
from sys.indexes i join sys.partition_schemes s on i.data_space_id = s.data_space_id};
	$sql .= $self->limit_to_objects('TABLE|PARTITION', 'TABLE_NAME|PARTITION_NAME');
	if ($self->{schema}) {
		$sql .= "\tAND object_schema_name(i.object_id) ='$self->{schema}'\n";
	}
	$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{'TABLE PARTITION'}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish;


	# User defined data types
	$sql = "select SCHEMA_NAME(schema_id)+'.'+Name  from sys.types where is_user_defined = 1";
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{'TYPE'}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	# TODO: other objects

	return %infos;
}

=head2 _table_info

This function retrieves all SQL Server tables information.

Returns a handle to a DB query statement.

=cut

sub _table_info
{
	# TODO: schéma pas pris en compte
	my $self = shift;

	# First register all table/filegroup in memory from this database
	my %tbspname = ();
	# MYSQL: my $sth = $self->{dbh}->prepare("SELECT DISTINCT TABLE_NAME, TABLESPACE_NAME FROM INFORMATION_SCHEMA.FILES WHERE table_schema = '$self->{schema}' AND TABLE_NAME IS NOT NULL") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0);
	my $sql = "select OBJECT_SCHEMA_NAME(t.object_id) + '.' + t.name, d.name AS FileGroup from sys.filegroups d inner join  sys.indexes i on  i.data_space_id = d.data_space_id inner JOIN sys.tables t ON t.object_id = i.object_id WHERE i.index_id<2";
	if ($self->{schema}) {
		$sql .= " and OBJECT_SCHEMA_NAME(t.object_id) = '$self->{schema}'";
	}
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $r = $sth->fetch) {
		$tbspname{$r->[0]} = $r->[1];
	}
	$sth->finish();


	my %tables_infos = ();
	my %comments = ();
	my $condition = '';
	if ($self->{schema}) {
		$condition = " and s.Name = '$self->{schema}'";
	}
	$condition .= $self->limit_to_objects('TABLE', 't.NAME');

	# my $sql = "SELECT TABLE_NAME,TABLE_COMMENT,TABLE_TYPE,TABLE_ROWS,ROUND( ( data_length + index_length) / 1024 / 1024, 2 ) AS \"Total Size Mb\", AUTO_INCREMENT, ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = '$self->{schema}'";
	my $sql = qq{SELECT s.Name + '.' + t.NAME AS TABLE_NAME, p.rows AS TABLE_ROWS, CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS TotalSpaceMB
FROM sys.tables t
INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.NAME NOT LIKE 'dt%' AND t.is_ms_shipped = 0 AND i.OBJECT_ID > 255 
$condition
GROUP BY t.Name, s.Name, p.Rows
ORDER BY t.Name};


	 #print "SQL=\n$sql\n\n";
	 $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
     $sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		# $tables_infos{$row->[0]}{owner} = '';
		$tables_infos{$row->[0]}{num_rows} = $row->[1] || 0;
		$tables_infos{$row->[0]}{size} = $row->[2] || 0;
		# $tables_infos{$row->[0]}{comment} =  $row->[1] ; # $comments{$row->[0]}{comment} || '';
		# $tables_infos{$row->[0]}{type} =  $comments{$row->[0]}{table_type} || '';
		# $tables_infos{$row->[0]}{nested} = '';
		# $tables_infos{$row->[0]}{tablespace} = 0;
		# $tables_infos{$row->[0]}{auto_increment} = $row->[5] || 0;
		# $tables_infos{$row->[0]}{tablespace} = $tbspname{$row->[0]} || '';
	}
	$sth->finish();

	# print "TABLE_infos ok\n";
	return %tables_infos;
}

=head2 _get_identities

This function retrieve information about IDENTITY columns that must be
exported as PostgreSQL serial.
not used this as Identity columns already converted to sequence

=cut

sub _get_identities
{
	my ($self) = @_;
	# nothing to do, Identity column are converted to sequence
	return;
	
#     my $str = "SELECT DISTINCT OBJECT_SCHEMA_NAME(sys.tables.object_id) SCHEMA_NAME,
# 			 OBJECT_SCHEMA_NAME(sys.tables.object_id)+'.'+OBJECT_NAME(SYS.IDENTITY_COLUMNS.OBJECT_ID) TableName,
# 			 SYS.IDENTITY_COLUMNS.NAME ColumnName,
# 			 'ALWAYS',         
# 			  ''
# 		FROM sys.columns
# 		INNER JOIN sys.tables ON sys.tables.object_id = sys.columns.object_id    AND sys.columns.is_identity = 1
# 		inner join  SYS.IDENTITY_COLUMNS on  SYS.IDENTITY_COLUMNS.object_id =  sys.columns.object_id";

#    if ($self->{schema}) {
# 		$str .= " WHERE OBJECT_SCHEMA_NAME(sys.tables.object_id) = '$self->{schema}'";
# 	}

# 	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
# 	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

# 	my %seqs = ();
# 	while (my $row = $sth->fetch) {
# 		if (!$self->{schema} && $self->{export_schema}) {
# 			$row->[1] = "$row->[0].$row->[1]";
# 		}
# 		# GENERATION_TYPE can be ALWAYS, BY DEFAULT and BY DEFAULT ON NULL
# 		$seqs{$row->[1]}{$row->[2]}{generation} = $row->[3];
# 		# SEQUENCE options
# 		$seqs{$row->[1]}{$row->[2]}{options} = $row->[4];
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/(START WITH):/$1/;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/(INCREMENT BY):/$1/;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/MAX_VALUE:/MAXVALUE/;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/MIN_VALUE:/MINVALUE/;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/CYCLE_FLAG: N/NO CYCLE/;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/CYCLE_FLAG: Y/CYCLE/;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/CACHE_SIZE:/CACHE/;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/CACHE_SIZE:/CACHE/;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/ORDER_FLAG: .//;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/,//g;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/\s$//;
# 		$seqs{$row->[1]}{$row->[2]}{options} =~ s/CACHE\s+0/CACHE 1/;
# 		if ( $seqs{$row->[1]}{$row->[2]}{options} eq 'START WITH 1 INCREMENT BY 1 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NO CYCLE CACHE 20') {
# 			delete $seqs{$row->[1]}{$row->[2]}{options};
# 		}
# 	}

# 	return %seqs;	
}

=head2 _table_exists
This function return the table name if the given table exists
else returns a empty string.

=cut

sub _table_exists
{
	my ($self, $schema, $table) = @_;
	# print "schema=[$schema], table=[$table]\n";
	my $ret = '';

	# my $sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table'";
	my $sql = "SELECT table_schema + '.' + table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' and TABLE_CATALOG='$schema' AND TABLE_NAME = '$table'";
	my $sth = $self->{dbh}->prepare( $sql ) or return undef;
	$sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$ret = $row[0];
		warn "SS table_exists ret=[$ret]\n";
	}
	$sth->finish();

	return $ret;
}

=head2 _get_indexes

This function implements an sql indexes information.

Returns a hash of an array containing all unique indexes and a hash of
array of all indexe names specified table.

=cut

sub _get_indexes
{
	# TODO: xml indexes
	# TODO: constraints on schema	
	# TODO: check carefully the following
	# type of index, number returned, tablespace
	my ($self, $table, $owner) = @_;

	my $condition = '';
	$condition .= " and OBJECT_SCHEMA_NAME(i.object_id) = '$self->{schema}'" if ($self->{schema});

	#$condition .= $self->limit_to_objects('TABLE', "OBJECT_NAME(i.object_id)")  # NOT USED !!!
	# $condition =~ s/ AND / WHERE /;

	my $sch = $self->{schema} . "." if ($self->{schema});
 
	# print "table=[$table] owner=[$owner] condition=[$condition]\n";

	my %tables_infos = ();
	if ($table) {
		$tables_infos{$table} = 1;
	} else {
		%tables_infos = Ora2Pg::SQLServer::_table_info($self);
	}

	# foreach(keys %tables_infos) { print "TI $_ / $tables_infos{$_}\n"; }

	my %data = ();
	my %unique = ();
	my %idx_type = ();
	my %index_tablespace = ();
	
	# Retrieve all indexes for the given table
	foreach my $t (keys %tables_infos) {
		my $sql = qq{
select OBJECT_SCHEMA_NAME(i.object_id) + '.' + OBJECT_NAME(i.object_id) As TABLE_NAME, 
	i.name as idx_name, 
	i.type_desc, 
	i.is_unique, 
	i.is_primary_key, 
	ic.column_id, 
	c.name, 
	fg.name as filegroupname
from sys.indexes i
	inner join sys.index_columns ic on i.object_id=ic.object_id and i.index_id=ic.index_id
	inner join sys.columns c on i.object_id=c.object_id and c.column_id = ic.column_id
	inner join sys.filegroups fg on fg.data_space_id = i.data_space_id
WHERE OBJECT_SCHEMA_NAME(i.object_id) + '.' + OBJECT_NAME(i.object_id) = '$t' and i.type_desc <> 'XML'
$condition
order by 1, 2, 5
};
	     #print "SS:GI:sql=$sql\n";
		my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			# print "SS:GI:table[$t]|index=[$row->[0]|[$row->[2]]\n";
			# print "SS:GetIndexes:CurrentIndex=[$row->[2]] \n";
	 		# next if ($row->[4] == 1);  # is_primary_key
	 		next if ($row->[3] == 1);  # is_unique ? TEST
			my $idxname = $row->[1];
			$unique{$row->[0]}{$idxname} = 'UNIQUE' if ($row->[3] == 1); # is_unique
			$idx_type{$row->[0]}{$idxname}{type_name} = "CLUSTER" if ($row->[2] eq 'CLUSTERED');     # TODO: should be corrected ?
			# $idx_type{$row->[0]}{$idxname}{type_name} = "CLUSTER" if ($row->[5] eq 'NONCLUSTERED');   
			# print "GI>type:[$row->[5])\n";

	 		push(@{$data{$row->[0]}{$idxname}}, $row->[6]);
	 		$index_tablespace{$row->[0]}{$idxname} = $row->[7];
		}
	}

	# Retrieve all indexes for the given table
	# foreach my $t (keys %tables_infos) {
	# 	my $sth = $self->{dbh}->prepare("SHOW INDEX FROM $t $condition;") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	# 	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	# 	my $i = 1;
	# 	while (my $row = $sth->fetch) {

	# 		next if ($row->[2] eq 'PRIMARY');
	#        	#Table #Non_unique #Key_name #Seq_in_index #Column_name #Collation #Cardinality #Sub_part #Packed #Null #Index_type #Comment 
	# 		my $idxname = $row->[2];
	# 		$row->[1] = 'UNIQUE' if (!$row->[1]);
	# 		$unique{$row->[0]}{$idxname} = $row->[1];
	# 		# Set right label to spatial index
	# 		if ($row->[10] =~ /SPATIAL/) {
	# 			$row->[10] = 'SPATIAL_INDEX';
	# 		}
	# 		$idx_type{$row->[0]}{$idxname}{type_name} = $row->[10];
	# 		# Save original column name
	# 		my $colname = $row->[4];
	# 		# Enclose with double quote if required
	# 		$row->[4] = $self->quote_object_name($row->[4]);

	# 		if ($self->{preserve_case}) {
	# 			if (($row->[4] !~ /".*"/) && ($row->[4] !~ /\(.*\)/)) {
	# 				$row->[4] =~ s/^/"/;
	# 				$row->[4] =~ s/$/"/;
	# 			}
	# 		}
	# 		push(@{$data{$row->[0]}{$idxname}}, $row->[4]);
	# 		$index_tablespace{$row->[0]}{$idxname} = '';

	# 	}
	# }

	
	# print "--------------------------\nSS: Get indexes - unique \n";
	# foreach(keys %unique) { print "TI $_ / $unique{$_}\n"; }
	# print "--------------------------\nSS: Get indexes - data \n";
	# foreach(keys %data) { print "TI $_ / $data{$_}\n"; }
	# print "--------------------------\nSS: Get indexes - idx_type \n";
	# foreach(keys %idx_type) { print "TI $_ / $idx_type{$_}\n"; }
	# print "--------------------------\nSS: Get indexes - index_tablespace \n";
	# foreach(keys %index_tablespace) { print "TI $_ / $index_tablespace{$_}\n"; }

	return \%unique, \%data, \%idx_type, \%index_tablespace;
}

=head2 _column_info


This function retrives all  column information.

Returns a list of array references containing the following information
elements for each column the specified table

[(
  column name,
  column type,
  column length,
  nullable column,
  default value
  ...
)]


=cut

sub _column_info
{
	# TODO: check conditions
	# TODO: COLUMN_TYPE ?
	# TODO: postgisg
	
	my ($self, $table, $owner, $objtype, $recurs) = @_;
	# print "SS:CI:table=[$table], owner=[$owner], objtype=[$objtype], recurs=[$recurs] \n";

	$objtype ||= 'TABLE';

	my $condition = '';
	if ($self->{schema}) {
		$condition .= " and TABLE_SCHEMA='$self->{schema}' ";
	}
	$condition .= " AND TABLE_NAME='$table' " if ($table);
	$condition .= $self->limit_to_objects('TABLE', 'TABLE_NAME') if (!$table);
	$condition =~ s/^AND/WHERE/;

	my $sql = qq{select COLUMN_NAME,DATA_TYPE,  CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_OCTET_LENGTH,  TABLE_SCHEMA + '.' + TABLE_NAME as TABLE_NAME, TABLE_SCHEMA as OWNER, '' AS VIRTUAL_COLUMN, ORDINAL_POSITION, ''  AS EXTRA, '' as COLUMN_TYPE
from INFORMATION_SCHEMA.COLUMNS  where 1 = 1
$condition
ORDER BY ORDINAL_POSITION
};

	my $sth = $self->{dbh}->prepare($sql);
	if (!$sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	my $pos = 0;
	# print "CI2\n";
	#	$tmp =~ s/(\s*,\s*),\s*/$1/gs;

	while (my $row = $sth->fetch) {
		# if bit and default, update default
		if (($row->[1] eq 'bit') && ($row->[4] ne '')) {
			$row->[4] =~ s/\((\d+)\)/'$1'::bit/;
		}
		$row->[10] = $pos;   # TODO: kept, but did not understand...
		push(@{$data{"$row->[8]"}{"$row->[0]"}}, @$row); # full tablename > column_name
		pop(@{$data{"$row->[8]"}{"$row->[0]"}});
		$pos++;
	}

	# print "---------\nCI2\n";
	# foreach(keys %data) { print "colInfo $_ / $data{$_}\n"; }

	return %data;
}

=head2 _column_comments

This function return comments associated to each column of table

=cut

sub _column_comments
{
	# TODO: tests with conditions
	my ($self, $table) = @_;
	my $condition = '';
	my %data = ();
	
	# print "SS:ColumnComments:table=[$table]\n";

	# my $sql = "SELECT COLUMN_NAME,COLUMN_COMMENT,TABLE_NAME,'' AS \"Owner\" FROM INFORMATION_SCHEMA.COLUMNS";
	my $sql = qq{Select 
sc.name as COLUMN_NAME, sep.value as COLUMN_COMMENT, schema_name(so.schema_id) + '.' + so.name as TABLE_NAME
From sys.extended_properties sep 
Inner join sys.objects so On sep.major_id = so.object_id 
inner join sys.columns sc On so.object_id = sc.object_id and sep.minor_id = sc.column_id 
Where sep.name = 'MS_Description' and so.type = 'U'};

	if ($self->{schema}) {
		$sql .= " AND schema_name(so.schema_id)='$self->{schema}' ";
	}
	$sql .= "AND so.name='$table' " if ($table);
	$sql .= $self->limit_to_objects('TABLE','so.name') if (!$table);

	my $sth = $self->{dbh}->prepare($sql) or $self->logit("WARNING only: " . $self->{dbh}->errstr . "\n", 0, 0);

	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		$data{$row->[2]}{$row->[0]} = $row->[1];
	}
	return %data;
}

=head2 _foreign_key

This function return Foreign key Information.

=cut

sub _foreign_key
{
	# TODO: check condition
	# TODO: missing info in %data ?
	
        my ($self, $table, $owner) = @_;
        my %data = ();
        my %link = ();
        my $condition = '';

	# print "SS:FK:table=[$table], owner=[$owner]\n";

        $condition .= " AND C.TABLE_NAME='$table' " if ($table);
        $condition .= " AND C.TABLE_SCHEMA='$self->{schema}' " if ($self->{schema});
		$condition .= $self->limit_to_objects('TABLE','C.TABLE_NAME') if (!$table);

        my $deferrable = $self->{fkey_deferrable} ? "'DEFERRABLE' AS DEFERRABLE" : "DEFERRABLE";

	# mysql version: 
	# my $sql = "SELECT DISTINCT A.COLUMN_NAME,A.ORDINAL_POSITION,A.TABLE_NAME,A.REFERENCED_TABLE_NAME,A.REFERENCED_COLUMN_NAME,A.POSITION_IN_UNIQUE_CONSTRAINT,A.CONSTRAINT_NAME,A.REFERENCED_TABLE_SCHEMA,B.MATCH_OPTION,B.UPDATE_RULE,B.DELETE_RULE FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS A INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS B ON A.CONSTRAINT_NAME = B.CONSTRAINT_NAME WHERE A.REFERENCED_COLUMN_NAME IS NOT NULL $condition ORDER BY A.ORDINAL_POSITION,A.POSITION_IN_UNIQUE_CONSTRAINT";

	my $sql = qq{
SELECT C.TABLE_SCHEMA as PK_SCHEMA, 
       C.TABLE_SCHEMA + '.' + C.TABLE_NAME  as PK_TABLE_NAME, 
       KCU.COLUMN_NAME PK_COLUMN_NAME, 
       C2.TABLE_SCHEMA as FK_TABLE_SCHEMA, 
       C2.TABLE_SCHEMA + '.' + C2.TABLE_NAME as FK_TABLE_NAME, 
       KCU2.COLUMN_NAME as FK_COLUMN_NAME, 
       C.CONSTRAINT_NAME as FK_NAME, 
       C2.CONSTRAINT_NAME as PK_NAME, 
       C.IS_DEFERRABLE,
       COL.COLUMN_NAME,
       C.TABLE_NAME as PK_TABLE_NAME_SHORT,
       RC.DELETE_RULE,
       RC.UPDATE_RULE
FROM   INFORMATION_SCHEMA.TABLE_CONSTRAINTS C 
       INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU ON C.CONSTRAINT_SCHEMA = KCU.CONSTRAINT_SCHEMA AND C.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME 
       INNER JOIN INFORMATION_SCHEMA.COLUMNS COL on COL.COLUMN_NAME = KCU.COLUMN_NAME AND COL.TABLE_SCHEMA = KCU.TABLE_SCHEMA and COL.TABLE_NAME = KCU.TABLE_NAME
       INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC ON C.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA AND C.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
       INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS C2 ON RC.UNIQUE_CONSTRAINT_SCHEMA = C2.CONSTRAINT_SCHEMA AND RC.UNIQUE_CONSTRAINT_NAME = C2.CONSTRAINT_NAME 
       INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2 ON C2.CONSTRAINT_SCHEMA = KCU2.CONSTRAINT_SCHEMA AND C2.CONSTRAINT_NAME = KCU2.CONSTRAINT_NAME AND KCU.ORDINAL_POSITION = KCU2.ORDINAL_POSITION 
WHERE  C.CONSTRAINT_TYPE = 'FOREIGN KEY'
$condition
ORDER BY COL.ORDINAL_POSITION 
};
        my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);
        my @cons_columns = ();
	my $i = 1;
        while (my $r = $sth->fetch) {
		# my $key_name = uc($r->[1] . '_' . $r->[2] . '_fk' . $i);
		my $key_name = uc($r->[6]);
		
		# if ($self->{schema} && (lc($r->[7]) ne lc($self->{schema}))) {
		# 	print STDERR "WARNING: Foreign key $r->[2].$r->[0] point to an other database: $r->[7].$r->[3].$r->[4], please fix it.\n";
		# }
		push(@{$link{$r->[1]}{$key_name}{local}}, $r->[2]);
		push(@{$link{$r->[1]}{$key_name}{remote}{$r->[4]}}, $r->[5]);
                # push(@{$data{$r->[4]}}, [ ($key_name, $key_name, $r->[8], $r->[10], 'DEFERRABLE', 'Y', '', $r->[2], '', $r->[9]) ]);  # TODO
		# $r->[8] = 'SIMPLE'; # See pathetical documentation of mysql
                push(@{$data{$r->[1]}}, [ ($key_name, $key_name, '', $r->[11] , 'DEFERRABLE', 'Y', '', $r->[1], '', $r->[12]) ]);  # TODO
		$i++;
        }
	$sth->finish();

	# print "LINK:\n\n";
	# print Dumper(\%link);
	# print "DATA\n\n";
	# print Dumper(\%data);

        return \%link, \%data;
}

=head2 _unique_key

TODO: add sub documentation

=cut

sub _unique_key
{
	my($self, $table, $owner, $type) = @_;

	my %result = ();

        my @accepted_constraint_types = ();

        push @accepted_constraint_types, "'P'" unless($self->{skip_pkeys});
        push @accepted_constraint_types, "'U'" unless($self->{skip_ukeys});
        return %result unless(@accepted_constraint_types);


	my $condition = '';
	$condition = " and  OBJECT_SCHEMA_NAME(i.object_id) ='$self->{schema}'" if ($self->{schema});
	$condition .= $self->limit_to_objects('TABLE', "OBJECT_NAME(i.object_id)") if (!$table);
	#$condition =~ s/ AND / WHERE /;

	my %tables_infos = ();
	if ($table) {
		$tables_infos{$table} = 1;
	} else {
		%tables_infos = Ora2Pg::SQLServer::_table_info($self);
	}
	
	foreach my $t (keys %tables_infos) {
		# my $sth = $self->{dbh}->prepare("SHOW INDEX FROM $t $condition;") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		# $sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

		my $type_of_constraint = '';
		if ($type == 'U') { $type_of_constraint = ' AND i.is_unique = 1 '}
		if ($type == 'P') { $type_of_constraint = ' AND i.is_primary_key = 1 '}

		my $sql = qq{
select OBJECT_SCHEMA_NAME(i.object_id) + '.' + OBJECT_NAME(i.object_id) As TABLE_NAME, 
	i.name as idx_name, 
	i.type_desc, 
	i.is_unique, 
	i.is_primary_key, 
	ic.column_id, 
	c.name, 
	fg.name as filegroupname
from sys.indexes i
	inner join sys.index_columns ic on i.object_id=ic.object_id and i.index_id=ic.index_id
	inner join sys.columns c on i.object_id=c.object_id and c.column_id = ic.column_id
	inner join sys.filegroups fg on fg.data_space_id = i.data_space_id
WHERE OBJECT_SCHEMA_NAME(i.object_id) + '.' + OBJECT_NAME(i.object_id) = '$t' and i.type_desc <> 'XML'
$condition
$type_of_constraint
order by 1, 2, 5
};
		my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

		my $i = 1;
		while (my $row = $sth->fetch) {
			# Exclude non unique constraints
			next if ($row->[3] == 0);

			my $idxname = $row->[0] . '_idx' . $i;
			$idxname = $row->[1];
			
			my $type = 'P';
			$type = 'U' if ($row->[4] == 0);

			next if (!grep(/^'$type'$/, @accepted_constraint_types));
			my $generated = 0;
			$generated = 'GENERATED NAME'; # if ($row->[4] == 0);
			if (!exists $result{$row->[0]}{$idxname}) {
				my %constraint = (type => $type, 'generated' => $generated, 'index_name' => $idxname, columns => [ ($row->[6]) ] );
				$result{$row->[0]}{$idxname} = \%constraint if ($row->[6]);
				$i++ if ($row->[4] == 0 );
			} else {
				push(@{$result{$row->[0]}{$idxname}->{columns}}, $row->[6]);
			}
		}
	}

	return %result
}

=head2 _count_indexes

When a diff is performed between SQL Server and PostgreSQL, this will count the
the number of indexes in the SQL Server database.

=cut

sub _count_indexes
{
	my ($self, $table, $owner) = @_;
	my %data = ();
	my $condition = '';
#--------------------------------------------

	my $condition = '';
	$condition .= " and OBJECT_SCHEMA_NAME(i.object_id) = '$self->{schema}'" if ($self->{schema});

	$condition .= $self->limit_to_objects('TABLE', "OBJECT_NAME(i.object_id)") if (!$table);  # NOT USED !!!
	# $condition =~ s/ AND / WHERE /;

	my $sch = $self->{schema} . "." if ($self->{schema});

	my %tables_infos = ();
	if ($table) {
		$tables_infos{$table} = 1;
	} else {
		%tables_infos = Ora2Pg::SQLServer::_table_info($self);
	}

	# foreach(keys %tables_infos) { print "TI $_ / $tables_infos{$_}\n"; }

	my %data = ();
	my %unique = ();
	my %idx_type = ();
	my %index_tablespace = ();
	
	# Retrieve all indexes for the given table
	foreach my $t (keys %tables_infos) {
		my $sql = qq{
select OBJECT_SCHEMA_NAME(i.object_id) + '.' + OBJECT_NAME(i.object_id) As TABLE_NAME, 
	i.name as idx_name, 
	i.type_desc, 
	i.is_unique, 
	i.is_primary_key, 
	ic.column_id, 
	c.name, 
	fg.name as filegroupname
from sys.indexes i
	inner join sys.index_columns ic on i.object_id=ic.object_id and i.index_id=ic.index_id
	inner join sys.columns c on i.object_id=c.object_id and c.column_id = ic.column_id
	inner join sys.filegroups fg on fg.data_space_id = i.data_space_id
WHERE OBJECT_SCHEMA_NAME(i.object_id) + '.' + OBJECT_NAME(i.object_id) = '$t' and i.type_desc <> 'XML'
$condition
order by 1, 2, 5
};
		my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
	 		push(@{$data{$row->[0]}{$row->[1]}}, $row->[6]);
		}
	}


#--------------------------------------------






	return \%data;
}

=head2 _count_sequences

When a diff is performed between SQL Server and PostgreSQL, this will count the
the number of identity columns in the SQL Server database.

=cut

sub _count_sequences
{
	my $self = shift;

     my @seqs = ();
	my $sql = "SELECT DISTINCT 	OBJECT_SCHEMA_NAME(sys.tables.object_id)+'.'+OBJECT_NAME(SYS.IDENTITY_COLUMNS.OBJECT_ID) TableName,
			SYS.IDENTITY_COLUMNS.NAME ColumnName
		FROM sys.columns
		INNER JOIN sys.tables ON sys.tables.object_id = sys.columns.object_id    AND sys.columns.is_identity = 1
		inner join  SYS.IDENTITY_COLUMNS on  SYS.IDENTITY_COLUMNS.object_id =  sys.columns.object_id";
	if ($self->{schema}) {
		$sql .= " WHERE OBJECT_SCHEMA_NAME(sys.tables.object_id) = '$self->{schema}'";
	}
	$sql .= $self->limit_to_objects('TABLE', 'OBJECT_NAME(SYS.IDENTITY_COLUMNS.OBJECT_ID)');
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		push(@seqs, $row->[0]) if ($row->[1]);
	}
	$sth->finish();
	
    return \@seqs;
}

=head2 _extract_sequence_info

This function retrieves the last value used from the indetity in the
sql database. The result is a sql script assigning the new start values
to the sequences found in the sql database.

=cut

sub _extract_sequence_info
{
	my ($self) = @_;

	my $sql = "SELECT DISTINCT OBJECT_NAME(sys.identity_columns.OBJECT_ID)+'_'+sys.identity_columns.NAME+'_seq' as SEQUENCE_NAME,         
			SEED_VALUE,
			null AS max_value,
			INCREMENT_VALUE,          
			ISNULL(LAST_VALUE,0) AS LAST_VALUE,
			1 AS CACHE_SIZE,
			null AS CYCLE_FLAG,
			OBJECT_SCHEMA_NAME(sys.tables.object_id) SEQUENCE_OWNER 
		FROM sys.columns
		INNER JOIN sys.tables ON sys.tables.object_id = sys.columns.object_id    AND sys.columns.is_identity = 1
		inner join  sys.identity_columns on sys.identity_columns.object_id =  sys.columns.object_id";


   if ($self->{schema}) {
		$sql .= " WHERE OBJECT_SCHEMA_NAME(sys.tables.object_id) = '$self->{schema}'";
	}
	#$sql .= $self->limit_to_objects('SEQUENCE','SEQUENCE_NAME');

	my @script = ();

	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr ."\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $seq_info = $sth->fetchrow_hashref) {

		my $seqname = $seq_info->{SEQUENCE_NAME};
		if (!$self->{schema} && $self->{export_schema}) {
			$seqname = $seq_info->{SEQUENCE_OWNER} . '.' . $seq_info->{SEQUENCE_NAME};
		}

		my $nextvalue = $seq_info->{LAST_NUMBER} + $seq_info->{INCREMENT_BY};
		my $alter = "ALTER SEQUENCE $self->{pg_supports_ifexists} " .  $self->quote_object_name($seqname) . " RESTART WITH $nextvalue;";
		push(@script, $alter);
		$self->logit("Extracted sequence information for sequence \"$seqname\"\n", 1);
	}
	$sth->finish();

	return @script;
}

=head2 _column_attributes

TODO: add sub documentation

=cut

sub _column_attributes
{
	my ($self, $table, $owner, $objtype) = @_;
	my %data = ();

	$objtype ||= 'TABLE';

	my $condition = '';
	if ($self->{schema}) {
		$condition .= "AND TABLE_SCHEMA='$self->{schema}' ";
	}
	$condition .= "AND TABLE_NAME='$table' " if ($table);
	$condition .= $self->limit_to_objects('TABLE', 'TABLE_NAME') if (!$table);
	$condition =~ s/^AND/WHERE/;


	my $sth = $self->{dbh}->prepare(<<END);
SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT, TABLE_SCHEMA + '.' + TABLE_NAME
FROM INFORMATION_SCHEMA.COLUMNS where 1 = 1
$condition
ORDER BY ORDINAL_POSITION
END
	if (!$sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch) {
		$data{$row->[3]}{"$row->[0]"}{nullable} = $row->[1];
		$data{$row->[3]}{"$row->[0]"}{default} = $row->[2];
	}

	return %data;

}

=head2 _encrypted_columns

TODO: add sub documentation

=cut

sub _encrypted_columns
{
	# Encrypted columns will be of varbinary type. 
	# Only by knowing which functions are used to decrypt/encrypt could we know.
        my($self) = @_;

	return;
}

=head2 _get_privilege

TODO: add sub documentation

=cut

sub _get_privilege
{
	# TODO: use GRANTOR column ?
	my($self) = @_;

	my %privs = ();
	my %roles = ();
	
	# Retrieve all privilege per table defined in this database
	my $str = "SELECT GRANTEE,TABLE_NAME,PRIVILEGE_TYPE,IS_GRANTABLE FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES where 1 = 1";
	if ($self->{schema}) {
		$str .= " and TABLE_SCHEMA = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('TABLE', 'TABLE_NAME');
	$str .= " ORDER BY TABLE_NAME, GRANTEE";
	my $error = "\n\nFATAL: You must be connected as an dbo user to retrieved grants\n\n"; #TODO: dbo ?
	my $sth = $self->{dbh}->prepare($str) or $self->logit($error . "FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		$privs{$row->[1]}{type} = $row->[2];
		if ($row->[3] eq 'YES') {
			$privs{$row->[1]}{grantable} = $row->[3];
		}
		$privs{$row->[1]}{owner} = '';
		push(@{$privs{$row->[1]}{privilege}{$row->[0]}}, $row->[2]);
		push(@{$roles{grantee}}, $row->[0]) if (!grep(/^$row->[0]$/, @{$roles{grantee}}));
	}
	$sth->finish();

	# Retrieve all privilege per column table defined in this database
	$str = "SELECT GRANTEE,TABLE_NAME,PRIVILEGE_TYPE,COLUMN_NAME,IS_GRANTABLE FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES where 1 = 1";
	if ($self->{schema}) {
		$str .= " and TABLE_SCHEMA = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('TABLE', 'TABLE_NAME');
	$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		# $row->[0] =~ s/\@.*//;  useless in SQL Server ?
		# $row->[0] =~ s/'//g;
		$privs{$row->[1]}{owner} = '';
		push(@{$privs{$row->[1]}{column}{$row->[3]}{$row->[0]}}, $row->[2]);
		push(@{$roles{grantee}}, $row->[0]) if (!grep(/^$row->[0]$/, @{$roles{grantee}}));
	}
	$sth->finish();

	return (\%privs, \%roles);
}

=head2 _get_security_definer

TODO: add sub documentation

=cut

sub _get_security_definer
{
	# TODO: verify SECURITY_TYPE and DEFINER
	my ($self, $type) = @_;
	my %security = ();
	
	# Retrieve all functions security information
	# my $str = "SELECT ROUTINE_NAME,ROUTINE_SCHEMA,SECURITY_TYPE,DEFINER FROM INFORMATION_SCHEMA.ROUTINES";
	my $str = "SELECT ROUTINE_NAME,ROUTINE_SCHEMA FROM INFORMATION_SCHEMA.ROUTINES where routine_type = 'PROCEDURE' and Left(Routine_Name, 3) NOT IN ('sp_', 'xp_', 'ms_')";
	if ($self->{schema}) {
		$str .= " AND ROUTINE_SCHEMA = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('FUNCTION|PROCEDURE', 'ROUTINE_NAME|ROUTINE_NAME');
	$str .= " ORDER BY ROUTINE_NAME";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch) {
		next if (!$row->[0]);
		$security{$row->[0]}{security} = 'DEFINER';
		$security{$row->[0]}{owner} = $row->[1];
	}
	$sth->finish();

	return (\%security);
}

=head2 _get_sequences

This function is used to retrieve all Identiy column information.
Returns a hash of an array of sequence names with MIN_VALUE, MAX_VALUE,
INCREMENT and LAST_NUMBER for the specified table.
=cut

sub _get_sequences
{
   my ($self) = @_;

    my $str = "SELECT DISTINCT OBJECT_NAME(sys.identity_columns.OBJECT_ID)+'_'+sys.identity_columns.NAME+'_seq' SEQUENCE_NAME,         
			SEED_VALUE,
			null as max_value,
			INCREMENT_VALUE,          
			ISNULL(LAST_VALUE,1) AS LAST_VALUE,
			1 as CACHE_SIZE,
			null as CYCLE_FLAG,
			OBJECT_SCHEMA_NAME(sys.tables.object_id) SCHEMA_NAME,
			OBJECT_SCHEMA_NAME(sys.tables.object_id)+'.'+OBJECT_NAME(sys.identity_columns.OBJECT_ID) TableName,
			sys.identity_columns.NAME ColumnName
		FROM sys.columns
		INNER JOIN sys.tables ON sys.tables.object_id = sys.columns.object_id    AND sys.columns.is_identity = 1
		inner join  sys.identity_columns on  sys.identity_columns.object_id =  sys.columns.object_id";		

   if ($self->{schema}) {
		$str .= " WHERE OBJECT_SCHEMA_NAME(sys.tables.object_id) = '$self->{schema}'";
	}

	$str .= $self->limit_to_objects('TABLE', 'OBJECT_NAME(sys.identity_columns.OBJECT_ID)');
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @seqs = ();
	while (my $row = $sth->fetch) {

		push(@seqs, [@$row]);
	}
	$sth->finish();

	return \@seqs;	
}

=head2 _get_dblink

TODO: add sub documentation

=cut

sub _get_dblink
{
	my($self) = @_;
	my %data = ();

	my $str = "select name, data_source from sys.servers where is_linked = 1";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch) {
		# if (!$self->{schema} && $self->{export_schema}) {
		# 	$row->[1] = "$row->[0].$row->[1]";
		# }
		# $data{$row->[0]}{owner} = $row->[0];
		# $data{$row->[1]}{username} = $row->[2];
		$data{$row->[0]}{host} = $row->[1];
	}

	#   my $str = "SELECT OWNER,DB_LINK,USERNAME,HOST FROM $self->{prefix}_DB_LINKS";
	#   if (!$self->{schema}) {
	#   	$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	#   } else {
	#   	$str .= " WHERE OWNER = '$self->{schema}'";
	#   }
	#   $str .= $self->limit_to_objects('DBLINK', 'DB_LINK');
	#   $str .= " ORDER BY DB_LINK";

	#   my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	#   $sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	#   while (my $row = $sth->fetch) {
	#   	if (!$self->{schema} && $self->{export_schema}) {
	#   		$row->[1] = "$row->[0].$row->[1]";
	#   	}
	#   	$data{$row->[1]}{owner} = $row->[0];
	#   	$data{$row->[1]}{username} = $row->[2];
	#   	$data{$row->[1]}{host} = $row->[3];
	#   }

	# print "\nSS_get_dblink\n";
	# print Dumper(\%data);

	return %data;
}

=head2 _get_job

TODO: add sub documentation

=cut

sub _get_job
{
	# TODO: limit + schema + info>what + info>interval
	my($self) = @_;
	my %data = ();
    return %data;
	# Retrieve all database job from user_jobs table
	# my $str = "SELECT EVENT_NAME,EVENT_DEFINITION,EXECUTE_AT FROM INFORMATION_SCHEMA.EVENTS WHERE STATUS = 'ENABLED'";
	my $str = "select name, description FROM msdb.dbo.sysjobs job where enabled = 1";
	# if ($self->{schema}) {
	# 	$str .= " AND EVENT_SCHEMA = '$self->{schema}'";
	# }
	# $str .= $self->limit_to_objects('JOB', 'EVENT_NAME');
	$str .= " ORDER BY NAME";
	# print "SS:GJ:sql=[$str]\n";	
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		# $data{$row->[0]}{what} = $row->[1];
		# $data{$row->[0]}{interval} = $row->[2];
		$data{$row->[0]}{what} = '';
		$data{$row->[0]}{interval} = '';
	}

	return %data;
}

=head2 _get_views

This function implements an sql views information.

Returns a hash of view names with the SQL queries they are based on.

=cut

sub _get_views
{
	# TODO: check limit + schema
	my ($self) = @_;
	my %data = ();

   
	# Retrieve comment of each columns
	# TABLE_CATALOG        | varchar(512) | NO   |     |         |       |
	# TABLE_SCHEMA         | varchar(64)  | NO   |     |         |       |
	# TABLE_NAME           | varchar(64)  | NO   |     |         |       |
	# VIEW_DEFINITION      | longtext     | NO   |     | NULL    |       |
	# CHECK_OPTION         | varchar(8)   | NO   |     |         |       |
	# IS_UPDATABLE         | varchar(3)   | NO   |     |         |       |
	# DEFINER              | varchar(77)  | NO   |     |         |       |
	# SECURITY_TYPE        | varchar(7)   | NO   |     |         |       |
	# CHARACTER_SET_CLIENT | varchar(32)  | NO   |     |         |       |
	# COLLATION_CONNECTION | varchar(32)  | NO   |     |         |       |
	my %comments = ();
	# Retrieve all views
	# my $str = "SELECT TABLE_NAME,VIEW_DEFINITION,CHECK_OPTION,IS_UPDATABLE,DEFINER,SECURITY_TYPE FROM INFORMATION_SCHEMA.VIEWS $condition";
	my $str = "select  [TABLE_SCHEMA], [TABLE_SCHEMA] + '.' + [TABLE_NAME], [VIEW_DEFINITION], [CHECK_OPTION], [IS_UPDATABLE] from INFORMATION_SCHEMA.VIEWS";
	$str .=  " where TABLE_SCHEMA='$self->{schema}' " if ($self->{schema});
	$str .= $self->limit_to_objects('VIEW', 'TABLE_NAME');
	$str .= " ORDER BY TABLE_NAME";
	$str =~ s/ AND / WHERE /;

	# print "SS:GetViews:sql=[$str]";
	
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @view_list; # results from first query
	while (my @row = $sth->fetchrow) {
		push(@view_list,\@row);
	}

	foreach my $v (@view_list)
	{
		# print "0=" . $v->[0] . "\n";
		# print "1=" . $v->[1] . "\n";

 		my $sql_ht = "exec sp_helptext '$v->[1]'";
 		my $sth_ht = $self->{dbh}->prepare($sql_ht);
 		$sth_ht->execute;
 		while (my $row_def = $sth_ht->fetch) {
			# print "\t$v->[1] = $row_def->[0] \n";
			$data{$v->[1]}{text} .= $row_def->[0];
			
 		}
	 	# The view_definition column contains "CREATE VIEW ... AS"
	 	$data{$v->[1]}{text} =~ s/^([\S\s])+?SELECT/SELECT/g;
	 	# We sometime find extra \M in view definition
	 	$data{$v->[1]}{text} =~ s///g;
	 
	 	$data{$v->[1]}{text} =~ s/`$self->{schema}`\.//g;
	 	$data{$v->[1]}{text} =~ s/`([^\s`,]+)`/$1/g;
	 	# $row->[2] =~ s/"/'/g;
	 	# $row->[2] =~ s/`/"/g;
		
 		$data{$v->[1]}{updatable} = $v->[4];
 		$data{$v->[1]}{owner} = '';
 		$data{$v->[1]}{comment} = '';
 		$data{$v->[1]}{check_option} = '';
 		$data{$v->[1]}{definer} = '';
 		$data{$v->[1]}{security} = '';

	}

# 	while (my $row = $sth->fetch) {
# 		# The view_definition column contains "CREATE VIEW ... AS"
# 		$row->[2] =~ s/([\S\s])+?SELECT/SELECT/g;
# 		# We sometime find extra \M in view definition
# 		$row->[2] =~ s///g;
# 
# 		$row->[2] =~ s/`$self->{schema}`\.//g;
# 		$row->[2] =~ s/`([^\s`,]+)`/$1/g;
# 		# $row->[2] =~ s/"/'/g;
# 		# $row->[2] =~ s/`/"/g;
# 
# 		# print "row0(text) = $row->[0] [$row->[3]]\n";
# 
# 		# we have to use sp_helptext as information_schema might not contain the whole definition
# 		my $sql_ht = "exec sp_helptext '$row->[1]'";
# 		my $sth_ht = $self->{dbh}->prepare($sql_ht);
# 		$sth_ht->execute;
# 		while (my $row_def = $sth->fetch) {
# 			print "\trow_def(2)=$row_def->[2] \n";
# 		}
# 
# 
# 		$data{$row->[1]}{text} = $row->[2];
# 		$data{$row->[1]}{updatable} = $row->[4];
# 		$data{$row->[1]}{owner} = '';
# 		$data{$row->[1]}{comment} = '';
# 		$data{$row->[1]}{check_option} = '';
# 		$data{$row->[1]}{definer} = '';
# 		$data{$row->[1]}{security} = '';
# 	}
	return %data;



}

=head2 _get_triggers

This function is used to retrieve all triggers information.

=cut

sub _get_triggers
{
	my($self) = @_;
	my @triggers = ();
	# my $str = "SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_STATEMENT, '' AS WHEN_CLAUSE, '' AS DESCRIPTION, ACTION_ORIENTATION FROM INFORMATION_SCHEMA.TRIGGERS";
	my $str = "SELECT o.name AS trigger_name ,  s.name + '.' + OBJECT_NAME(o.parent_object_id) AS table_name, '', '', OBJECT_DEFINITION (o.object_id) AS [Trigger Definition] FROM sys.objects o INNER JOIN sys.tables t ON o.parent_object_id = t.object_id INNER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE o.type = 'TR' ";
	if ($self->{schema}) {
		$str .= " AND s.name = '$self->{schema}'";
	}
	#  $str .= " " . $self->limit_to_objects('TABLE|VIEW|TRIGGER','EVENT_OBJECT_TABLE|EVENT_OBJECT_TABLE|TRIGGER_NAME');
	#  $str =~ s/ AND / WHERE /;

	$str .= " ORDER BY TABLE_NAME, TRIGGER_NAME";

	# print "SS:GT:sql=[$str]\n";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @triggers = ();
	while (my $row = $sth->fetch) {
		# $row->[7] = 'FOR EACH '. $row->[7];
		push(@triggers, [ @$row ]);
	}

	return \@triggers;
}

=head2 _list_triggers

TODO: add sub documentation

=cut

sub _list_triggers
{
        my($self) = @_;
	my %triggers = ();

	my $str = qq{
SELECT 
     sysobjects.name AS TRIGGER_NAME 
    ,USER_NAME(sysobjects.uid) AS TRIGGER_OWNER 
    ,s.name AS TABLE_SCHEMA 
    ,OBJECT_NAME(parent_obj) AS TABLE_NAME 
FROM sysobjects 
INNER JOIN sysusers 
    ON sysobjects.uid = sysusers.uid 
INNER JOIN sys.tables t 
    ON sysobjects.parent_obj = t.object_id 
INNER JOIN sys.schemas s 
    ON t.schema_id = s.schema_id 
WHERE sysobjects.type = 'TR' 
	};

	# my $str = "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE FROM INFORMATION_SCHEMA.TRIGGERS";
	if ($self->{schema}) {
		$str .= " AND TRIGGER_SCHEMA = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('TABLE','OBJECT_NAME(parent_obj)');
	$str =~ s/ AND / WHERE /;

	$str .= " ORDER BY TABLE_NAME, TRIGGER_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch) {
		push(@{$triggers{$row->[3]}}, $row->[0]);
	}

	return %triggers;



}

=head2 _get_plsql_metadata

TODO: add sub documentation

=cut

sub _get_plsql_metadata
{
        my $self = shift;
        my $owner = shift;

}

=head2 _get_functions

This function is used to retrieve all function information.

=cut

sub _get_functions
{

	
	my ($self) = @_;
	my %functions = ();

	my $condition = '';
	if ($self->{schema}) {
		$condition .= " AND SCHEMA_NAME(o.schema_id) = '$self->{schema}'";
		 }


	# my $str = "SELECT ROUTINE_NAME,ROUTINE_DEFINITION,DATA_TYPE,ROUTINE_BODY,EXTERNAL_LANGUAGE,SECURITY_TYPE,IS_DETERMINISTIC FROM INFORMATION_SCHEMA.ROUTINES";
	my $str = qq{ SELECT OBJECT_NAME(sm.object_id) AS function_name,  SCHEMA_NAME(o.schema_id) as schema_name ,  sm.definition as function_definition
FROM sys.sql_modules AS sm  
JOIN sys.objects AS o ON sm.object_id = o.object_id  
WHERE o.type_desc like '%_FUNCTION'
$condition
ORDER BY o.type};


	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	# print "SS:GF:sql=[$str]\n";


		while (my $row = $sth->fetch) {
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
	
		$functions{"$row->[0]"}{name} = $row->[0];
		$functions{"$row->[0]"}{owner} = $row->[1];
		$functions{"$row->[0]"}{text} .= $row->[2];
		
	}

	return \%functions;

}

=head2 _get_procedures

This function is used to retrieve all procedures information.

=cut

sub _get_procedures
{
	# TODO: limit to objects and schema
	my $self = shift;
	my %procedures = ();

	my $condition = '';
	if ($self->{schema}) {
		$condition .= " AND SCHEMA_NAME(o.schema_id) = '$self->{schema}'";
		 }

	# my $str = "SELECT ROUTINE_NAME,ROUTINE_DEFINITION,DATA_TYPE,ROUTINE_BODY,EXTERNAL_LANGUAGE,SECURITY_TYPE,IS_DETERMINISTIC FROM INFORMATION_SCHEMA.ROUTINES";
	my $str = qq{ SELECT OBJECT_NAME(sm.object_id) AS procedure_name,   o.type_desc,   sm.definition as procedure_definition,  SCHEMA_NAME(o.schema_id) as schema_name
FROM sys.sql_modules AS sm  
JOIN sys.objects AS o ON sm.object_id = o.object_id  
WHERE o.type_desc='SQL_STORED_PROCEDURE'
$condition
ORDER BY o.type};

	
	# $str .= " " . $self->limit_to_objects('FUNCTION','ROUTINE_NAME');
	# $str =~ s/ AND / WHERE /;
	# $str .= " ORDER BY ROUTINE_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	# print "SS:GP:sql=[$str]\n";

	while (my $row = $sth->fetch) {
		# print "SS:GP:row0=[$row->[0]] | def=[$row->[2]]\n";
		if ($self->{plsql_pgsql} || ($self->{type} eq 'SHOW_REPORT')) {

			if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[3].$row->[0]";
			}

			$procedures{"$row->[0]"}{name} = $row->[0];							
			$procedures{"$row->[0]"}{text} = $row->[2];
			$procedures{"$row->[0]"}{owner} = $row->[3];
		
			# $procedures{"$row->[0]"}{language} = $row->[3];
			# $procedures{"$row->[0]"}{security} = $row->[5];
			# $procedures{"$row->[0]"}{immutable} = $row->[6];
		}
	}

	return \%procedures;
}

=head2 _global_temp_table_info

This function retrive all global temp tables used in stored procedures along with Proc name

=cut

sub _global_temp_table_info
{
	# TODO: filter objects + schema
        my($self) = @_;

	my $sql = "SELECT DISTINCT  o.type_desc, o.name AS Object_Name,  m.definition  FROM sys.sql_modules m
       INNER JOIN       sys.objects o          ON m.object_id = o.object_id WHERE m.definition Like '%##%'";
	
     my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %tables_infos = ();
	while (my $row = $sth->fetch) {
		# if (!$self->{schema} && $self->{export_schema}) {
		# 	$row->[1] = "$row->[0].$row->[1]";
		# }
		# $tables_infos{$row->[1]}{owner} = $row->[0] || '';
		# $tables_infos{$row->[1]}{num_rows} = $row->[2] || 0;
		# $tables_infos{$row->[1]}{tablespace} = $row->[3] || 0;
		# $tables_infos{$row->[1]}{comment} =  $comments{$row->[1]}{comment} || '';
		$row->[1] = $row->[1]."\n";
		while ( $row->[2] =~ /\s*create\s+table\s+(##\w*)/gip)  # OK sans tags
		{
			$row->[1] = $row->[1].$1."\n"
		}
		$tables_infos{$row->[1]}{type} =  ''; 
		# BEFORE: $comments{$row->[1]}{table_type} || '';
		# $tables_infos{$row->[1]}{nested} = $row->[4] || '';
		# if ($row->[5] eq 'NO') {
		# 	$tables_infos{$row->[1]}{nologging} = 1;
		# } else {
		# 	$tables_infos{$row->[1]}{nologging} = 0;
		# }
		# $tables_infos{$row->[1]}{num_rows} = 0;
	}
	$sth->finish();

	return %tables_infos;
}

=head2 _get_tablespaces

TODO: add sub documentation

=cut

sub _get_tablespaces
{
	my ($self) = shift;

	return;
}

=head2 _list_tablespaces

TODO: add sub documentation

=cut

sub _list_tablespaces
{
	my ($self) = shift;

	return;
}

=head2 _get_partitions

TODO: add sub documentation

=cut

sub _get_partitions
{
	my($self) = @_;
	my %parts = ();
	my %default = ();

	return \%parts, \%default;
}

=head2 _get_subpartitions

TODO: add sub documentation

=cut

sub _get_subpartitions
{
	# SQL Server does not support sub partitions
	my($self) = @_;
	my %subparts = ();
	my %default = ();

	return \%subparts, \%default;
}

=head2 _get_synonyms

TODO: add sub documentation

=cut

sub _get_synonyms
{
	#TODO: schemaname
	my ($self) = shift;
	my %synonyms = ();

	# my $str = "SELECT OWNER,SYNONYM_NAME,TABLE_OWNER,TABLE_NAME,DB_LINK FROM $self->{prefix}_SYNONYMS";
	my $str = "SELECT name, COALESCE(PARSENAME(base_object_name,2),SCHEMA_NAME(SCHEMA_ID())) AS schemaName, PARSENAME(base_object_name,1) AS objectName FROM sys.synonyms ORDER BY NAME";

	# if ($self->{schema}) {
	# 	$str .= " WHERE (owner='$self->{schema}' OR owner='PUBLIC') AND table_owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	# } else {
	# 	$str .= " WHERE (owner='PUBLIC' OR owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "')) AND table_owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	# }
	# $str .= $self->limit_to_objects('SYNONYM','SYNONYM_NAME');
	# $str .= " ORDER BY SYNONYM_NAME\n";


	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %synonyms = ();
	while (my $row = $sth->fetch) {
		# next if ($row->[1] =~ /^\//); # Some not fully deleted synonym start with a slash
		# if (!$self->{schema} && $self->{export_schema}) {
		# 	$row->[1] = "$row->[0].$row->[1]";
		# }
		# $synonyms{$row->[1]}{owner} = $row->[0];
		# $synonyms{$row->[1]}{table_owner} = $row->[2];
		$synonyms{$row->[0]}{table_name} = $row->[1] . '.' . $row->[2];
		# $synonyms{$row->[1]}{dblink} = $row->[4];
	}
	$sth->finish;

	return %synonyms;
}

=head2 _get_partitions_list

TODO: add sub documentation

=cut

sub _get_partitions_list
{
	my($self) = @_;
	my %parts = ();



	# Retrieve all partitions.
# 	my $str = qq{
# SELECT TABLE_NAME, PARTITION_ORDINAL_POSITION, PARTITION_NAME, PARTITION_DESCRIPTION, TABLESPACE_NAME, PARTITION_METHOD
# FROM INFORMATION_SCHEMA.PARTITIONS WHERE SUBPARTITION_NAME IS NULL AND PARTITION_NAME IS NOT NULL
# };
	my $str = qq{
select object_schema_name(i.object_id)  + '.' + object_name(i.object_id) as TABLE_NAME, i.name as INDEX_NAME, s.name as PARTITION_SCHEME
from sys.indexes i
join sys.partition_schemes s on i.data_space_id = s.data_space_id};

	$str .= $self->limit_to_objects('TABLE','object_name(i.object_id)');
	if ($self->{schema}) {
		$str .= " AND object_schema_name(i.object_id) ='$self->{schema}'";
	}
	$str .= " ORDER BY TABLE_NAME,PARTITION_SCHEME\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch) {
		$parts{$row->[5]}++;
	}
	$sth->finish;

	return %parts;
}

=head2 _get_partitioned_table

TODO: add sub documentation

=cut

sub _get_partitioned_table
{
	my($self) = @_;
	my %parts = ();
	
	#   # Retrieve all partitions.
	#   my $str = qq{ SELECT TABLE_NAME, PARTITION_ORDINAL_POSITION, PARTITION_NAME, PARTITION_DESCRIPTION, TABLESPACE_NAME, PARTITION_METHOD FROM INFORMATION_SCHEMA.PARTITIONS WHERE SUBPARTITION_NAME IS NULL AND PARTITION_NAME IS NOT NULL };
	my $str = qq{ 
select object_schema_name(i.object_id)  + '.' + object_name(i.object_id) as TABLE_NAME, i.name as INDEX_NAME, s.name as PARTITION_SCHEME, pf.fanout as PARTITION_NUMBER, object_schema_name(i.object_id) as TABLE_SCHEMA 
from sys.indexes i 
join sys.partition_schemes s on i.data_space_id = s.data_space_id 
join sys.partition_functions pf on pf.function_id = s.function_id
};
	$str .= $self->limit_to_objects('TABLE','object_name(i.object_id)');
	if ($self->{schema}) {
		$str .= " AND object_schema_name(i.object_id) ='$self->{schema}'";
	}
	$str .= " ORDER BY TABLE_NAME,s.name\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch) {
		# $parts{$row->[0]}++ if ($row->[2]);
		$parts{$row->[0]} = $row->[3];
		$parts{"\L$row->[0]\E"}{count} = $row->[3];
	}
	$sth->finish;

	return %parts;
}

=head2 _get_subpartitioned_table

TODO: add sub documentation

=cut

sub _get_subpartitioned_table
{
        my($self) = @_;

	return;
}

=head2 _get_largest_tables

TODO: add sub documentation

=cut

sub _get_largest_tables
{
	# TODO: limit to objects
    my ($self) = @_;

	my %table_size = ();
	my $condition = '';
	if ($self->{schema}) {
		$condition .= " AND object_schema_name(i.object_id) ='$self->{schema}'";
	}
    $condition .= $self->limit_to_objects('TABLE','t.NAME');

	my $sql = "SELECT";
	$sql .= " TOP($self->{top_max})" if ($self->{top_max});
	$sql .= qq{
s.Name + '.' + t.NAME AS TABLE_NAME, CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS TotalSpaceMB
FROM sys.tables t
INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.NAME NOT LIKE 'dt%' AND t.is_ms_shipped = 0 AND i.OBJECT_ID > 255 
$condition
GROUP BY  s.Name + '.' + t.Name, p.Rows
ORDER BY TotalSpaceMB desc 
};
	# $sql .= $self->limit_to_objects('TABLE', 'TABLE_NAME');
	# $sql .= " GROUP BY TABLE_NAME ORDER BY tsize";

	# print "SS:Biggesttables:sql=[$sql]\n";

        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$table_size{$row[0]} = $row[1];
	}
	$sth->finish();

	return %table_size;
}

=head2 replace_sql_type

TODO: add sub documentation

=cut

sub replace_sql_type
{
	# TODO: certainly not working, copied from MYSQL::replace_sql_type
	# print "SS:replace_SQL_type\n";
    my ($str, $pg_numeric_type, $default_numeric, $pg_integer_type, %data_type) = @_;
	
	# Replace type with precision
	my $sqlservertype_regex = '';
	foreach (keys %data_type) {
		$sqlservertype_regex .= quotemeta($_) . '|';
	}
	$sqlservertype_regex =~ s/\|$//;
	
	while ($str =~ /(.*)\b($sqlservertype_regex)\s*\(([^\)]+)\)/i) {
		my $backstr = $1;
		my $type = uc($2);
		my $args = $3;
		# if (uc($type) eq 'ENUM') {
		# 	# Prevent from infinit loop
		# 	$str =~ s/\(/\%\|/s;
		# 	$str =~ s/\)/\%\|\%/s;
		# 	next;
		# }
		if (exists $data_type{"$type($args)"}) {
			$str =~ s/\b$type\($args\)/$data_type{"$type($args)"}/igs;
			next;
		}
		if ($backstr =~ /_$/) {
		    $str =~ s/\b($sqlservertype_regex)\s*\(([^\)]+)\)/$1\%\|$2\%\|\%/is;
		    next;
		}

		my ($precision, $scale) = split(/,/, $args);
		$scale ||= 0;
		my $len = $precision || 0;
		$len =~ s/\D//;
		if ( $type =~ /CHAR/i ) {
			# Type CHAR have default length set to 1
			# Type VARCHAR must have a specified length
			$len = 1 if (!$len && ($type eq "CHAR"));    # TODO: check this
			$str =~ s/\b$type\b\s*\([^\)]+\)/$data_type{$type}\%\|$len\%\|\%/is;    # TODO: check this
		} elsif ($precision && ($type =~ /(BIT|TINYINT|SMALLINT|MEDIUMINT|INTEGER|BIGINT|INT|REAL|DOUBLE|FLOAT|DECIMAL|NUMERIC)/)) {
			if (!$scale) {
				if ($type =~ /(BIT|TINYINT|SMALLINT|MEDIUMINT|INTEGER|BIGINT|INT)/) {
					if ($pg_integer_type) {
						if ($precision < 5) {
							$str =~ s/\b$type\b\s*\([^\)]+\)/smallint/is;
						} elsif ($precision <= 9) {
							$str =~ s/\b$type\b\s*\([^\)]+\)/integer/is;
						} else {
							$str =~ s/\b$type\b\s*\([^\)]+\)/bigint/is;
						}
					} else {
						$str =~ s/\b$type\b\s*\([^\)]+\)/numeric\%\|$precision\%\|\%/i;
					}
				} else {
					$str =~ s/\b$type\b\s*\([^\)]+\)/$data_type{$type}\%\|$precision\%\|\%/is;
				}
			} else {
				if ($type =~ /DOUBLE/) {
					$str =~ s/\b$type\b\s*\([^\)]+\)/decimal\%\|$args\%\|\%/is;
				} else {
					$str =~ s/\b$type\b\s*\([^\)]+\)/$data_type{$type}\%\|$args\%\|\%/is;
				}
			}
		} else {
			# Prevent from infinit loop
			$str =~ s/\(/\%\|/s;
			$str =~ s/\)/\%\|\%/s;
		}
	}
	$str =~ s/\%\|\%/\)/gs;
	$str =~ s/\%\|/\(/gs;
	
	my %recover_type = ();
	my $i = 0;
	foreach my $type (sort { length($b) <=> length($a) } keys %data_type) {
		# Keep enum as declared, we are not in table definition
		next if (uc($type) eq 'ENUM');
		while ($str =~ s/\b$type\b/%%RECOVER_TYPE$i%%/is) {
			$recover_type{$i} = $data_type{$type};
			$i++;
		}
	}
	foreach $i (keys %recover_type) {
		$str =~ s/\%\%RECOVER_TYPE$i\%\%/$recover_type{$i}/;
	}


	return $str;
}

=head2 _get_audit_queries

TODO: add sub documentation

=cut

sub _get_audit_queries
{
	my($self) = @_;
	my %queries = ();

	return if (!$self->{audit_user});

	return %queries;
}

=head2 _sql_type

TODO: add sub documentation

=cut

sub _sql_type
{
	# TODO: taken from MySQL and not modified yet
	# TODO: precision -1 is equivalent to "MAX" and might need a change to other type
        my ($self, $type, $len, $precision, $scale) = @_;
	# print "\t$type, $len, $precision, $scale\n"; # TODOANTHONY
	my $data_type = '';

	# # Simplify timestamp type
	# $type =~ s/TIMESTAMP\s*\(\s*\d+\s*\)/TIMESTAMP/i;
	# $type =~ s/TIME\s*\(\s*\d+\s*\)/TIME/i;
	# $type =~ s/DATE\s*\(\s*\d+\s*\)/DATE/i;
	# # Remove BINARY from CHAR(n) BINARY, TEXT(n) BINARY, VARCHAR(n) BINARY ...
	# $type =~ s/(CHAR|TEXT)\s*(\(\s*\d+\s*\)) BINARY/$1$2/i;
	# $type =~ s/(CHAR|TEXT)\s+BINARY/$1/i;

	# Some length and scale may have not been extracted before
	if ($type =~ s/\(\s*(\d+)\s*\)//) {
		$len   = $1;
	} elsif ($type =~ s/\(\s*(\d+)\s*,\s*(\d+)\s*\)//) {
		$len   = $1;
		$scale = $2;
	}
	if ($type !~ /CHAR/i) {
		$precision = $len if (!$precision);
	}

        # Override the length
        $len = $precision if ( ((uc($type) eq 'NUMBER') || (uc($type) eq 'BIT')) && $precision );
        if (exists $self->{data_type}{uc($type)}) {
		$type = uc($type); # Force uppercase
		# print "\n\tss:sqltype: type=[$type], len=[$len]\n";
		if ($len) {
			if ( ($type eq "CHAR") || ($type eq "NCHAR") || ($type =~ /VARCHAR/) ) {   # TODO: anthony> ajout nchar
				# [N]VARCHAR(-1) should be changed to TEXT instead
				if ($len == -1) { 
					# print "Found *char(max) \n";
					# $len == 4000; 				
					# return "$self->{data_type}{$type}(4000)";
					return "$self->{data_type}{'TEXT'}";
				}

				# Type CHAR have default length set to 1
				# Type VARCHAR(2) must have a specified length
				$len = 1 if (!$len && ($type eq "CHAR"));
                		return "$self->{data_type}{$type}($len)";
			} elsif ($type eq 'BIT') {
				if ($precision) {
					return "$self->{data_type}{$type}($precision)";
				} else {
					return $self->{data_type}{$type};
				}
			} elsif ($type =~ /(TINYINT|SMALLINT|MEDIUMINT|INTEGER|BIGINT|INT|REAL|DOUBLE|FLOAT|DECIMAL|NUMERIC)/i) {
				# This is an integer
				if (!$scale) {
					if ($precision) {
						if ($self->{pg_integer_type}) {
							if ($precision < 5) {
								return 'smallint';
							} elsif ($precision <= 9) {
								return 'integer'; # The speediest in PG
							} else {
								return 'bigint';
							}
						}
						return "numeric($precision)";
					} else {
						# Most of the time interger should be enought?
						return $self->{data_type}{$type};
					}
				} else {
					if ($precision) {
						if ($type !~ /DOUBLE/ && $self->{pg_numeric_type}) {
							if ($precision <= 6) {
								return 'real';
							} else {
								return 'double precision';
							}
						}
						return "decimal($precision,$scale)";
					}
				}
			}
			return $self->{data_type}{$type};
		} else {
			return $self->{data_type}{$type};
		}
        }

        return $type;
}

=head2 replace_sqlserver_variables

TODO: add sub documentation

=cut

sub replace_sqlserver_variables
{
	my ($self, $code, $declare) = @_;

	# Look for mysql global variables and add them to the custom variable list
	while ($code =~ s/\b(?:SET\s+)?\@\@(?:SESSION\.)?([^\s:=]+)\s*:=\s*([^;]+);/PERFORM set_config('$1', $2, false);/is) {
		my $n = $1;
		my $v = $2;
		$self->{global_variables}{$n}{name} = lc($n);
		# Try to set a default type for the variable
		$self->{global_variables}{$n}{type} = 'bigint';
		if ($v =~ /'[^\']*'/) {
			$self->{global_variables}{$n}{type} = 'varchar';
		}
		if ($n =~ /datetime/i) {
			$self->{global_variables}{$n}{type} = 'timestamp';
		} elsif ($n =~ /time/i) {
			$self->{global_variables}{$n}{type} = 'time';
		} elsif ($n =~ /date/i) {
			$self->{global_variables}{$n}{type} = 'date';
		} 
	}

	my @to_be_replaced = ();
	# Look for local variable definition and append them to the declare section
	while ($code =~ s/SET\s+\@([^\s:]+)\s*:=\s*([^;]+);/SET $1 = $2;/is) {
		my $n = $1;
		my $v = $2;
		# Try to set a default type for the variable
		my $type = 'integer';
		$type = 'varchar' if ($v =~ /'[^']*'/);
		if ($n =~ /datetime/i) {
			$type = 'timestamp';
		} elsif ($n =~ /time/i) {
			$type = 'time';
		} elsif ($n =~ /date/i) {
			$type = 'date';
		} 
		$declare .= "$n $type;\n" if ($declare !~ /\b$n $type;/s);
		push(@to_be_replaced, $n);
	}

	# Look for local variable definition and append them to the declare section
	while ($code =~ s/(\s+)\@([^\s:=]+)\s*:=\s*([^;]+);/$1$2 := $3;/is) {
		my $n = $2;
		my $v = $3;
		# Try to set a default type for the variable
		my $type = 'integer';
		$type = 'varchar' if ($v =~ /'[^']*'/);
		if ($n =~ /datetime/i) {
			$type = 'timestamp';
		} elsif ($n =~ /time/i) {
			$type = 'time';
		} elsif ($n =~ /date/i) {
			$type = 'date';
		} 
		$declare .= "$n $type;\n" if ($declare !~ /\b$n $type;/s);
		push(@to_be_replaced, $n);
	}

	# Fix other call to the same variable in the code
	foreach my $n (@to_be_replaced) {
		$code =~ s/\@$n\b(\s*[^:])/$n$1/gs;
	}

	# Look for local variable definition and append them to the declare section
	while ($code =~ s/\@([a-z0-9_]+)/$1/is) {
		my $n = $1;
		# Try to set a default type for the variable
		my $type = 'varchar';
		if ($n =~ /datetime/i) {
			$type = 'timestamp';
		} elsif ($n =~ /time/i) {
			$type = 'time';
		} elsif ($n =~ /date/i) {
			$type = 'date';
		} 
		$declare .= "$n $type;\n" if ($declare !~ /\b$n $type;/s);
		# Fix other call to the same variable in the code
		$code =~ s/\@$n\b/$n/gs;
	}

	# Look for variable definition with SELECT statement
	$code =~ s/\bSET\s+([^\s=]+)\s*=\s*([^;]+\bSELECT\b[^;]+);/$1 = $2;/igs;

	return ($code, $declare);
}

=head2 _list_all_funtions

TODO: add sub documentation

=cut

sub _list_all_funtions
{
	my $self = shift;
	my @functions = ();

	return @functions;
}

=head2 _lookup_function

TODO: add sub documentation

=cut

sub _lookup_function
{
	
	my ($self, $code, $fctname) = @_;

	my $type = lc($self->{type}) . 's';

	# Replace all double quote with single quote
	$code =~ s/"/'/g;
	# replace backquote with double quote
	$code =~ s/`/"/g;
	
        my %fct_detail = ();
        $fct_detail{func_ret_type} = 'OPAQUE';

        # Split data into declarative and code part
        ($fct_detail{declare}, $fct_detail{code}) = split(/\bBEGIN\b/i, $code, 2);
	    return if (!$fct_detail{code});

	 # Remove any label that was before the main BEGIN block
	   $fct_detail{declare} =~ s/\s+[^\s\:]+:\s*$//gs;

	   	#replace [ or ] with empty ex "[dbo].[Test] replace dbo.Test"
		$fct_detail{declare} =~ s/[\[\|\]\*]//g;

        @{$fct_detail{param_types}} = ();

        if ( ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE)\s+([^\s\(]+)\s*(\(.*\))\s*RETURNS\s*(.*)AS\s*//is) ||
        ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE)\s+([^\s\(]+)\s*(\(?.*\)?)//is) ) 
		{
                $fct_detail{before} = $1;
                $fct_detail{type} = uc($2);
                $fct_detail{name} = $3;
                $fct_detail{args} = $4;
				my $tmp_returned = $5;
				chomp($tmp_returned);

		if ($tmp_returned =~ s/\b(DECLARE\b.*)//is) {
			$fct_detail{code} = $1 . $fct_detail{code};
		}
		if ($fct_detail{declare} =~ s/\s*COMMENT\s+(\?TEXTVALUE\d+\?|'[^\']+')//) {
			$fct_detail{comment} = $1;
		}

		#repalce spaces with empty after data type
		$tmp_returned =~s/(\w*)\s+$/$1/g;

		$fct_detail{before} = ''; # There is only garbage for the moment

		#replace ' and  " with empty for name 
		$fct_detail{name} =~ s/['"]//g;

	
		$fct_detail{fct_name} = $fct_detail{name};
		if (!$fct_detail{args}) {
			$fct_detail{args} = '()';		}
	

		$fctname = $fct_detail{name} || $fctname;
		if ($type eq 'functions' && exists $self->{$type}{$fctname}{return} && $self->{$type}{$fctname}{return}) {
			$fct_detail{hasreturn} = 1;
			$fct_detail{func_ret_type} = $self->_sql_type($self->{$type}{$fctname}{return});
		} elsif ($type eq 'functions' && !exists $self->{$type}{$fctname}{return} && $tmp_returned) {
			$tmp_returned =~ s/\s+CHARSET.*//is;
			$fct_detail{func_ret_type} = $self->_sql_type($tmp_returned);
			$fct_detail{hasreturn} = 1;
		}
		if($fct_detail{func_ret_type} =~ /\s*(.*)Table\s*/i)
		{
			$fct_detail{func_ret_type} = replace_sql_type($fct_detail{func_ret_type}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type}, %{ $self->{data_type} });
		}
		$fct_detail{language} = $self->{$type}{$fctname}{language};	

		# Procedure that have out parameters are functions with PG
		if ($type eq 'procedures' && $fct_detail{args} =~ /\b(OUT|INOUT)\b/) {
			# set return type to empty to avoid returning void later
			$fct_detail{func_ret_type} = ' ';
		}
		# IN OUT should be INOUT
		$fct_detail{args} =~ s/\bIN\s+OUT/INOUT/igs;
		
		#replace @varibl with p_name for input paramters and l_name for inside code variables
		while ( $fct_detail{args} =~ /\@([^\s]+)/g)  # OK sans tags
		{
			    $fct_detail{args}=~ s/\@($1)/p_$1/gs;
			    $fct_detail{code}=~ s/\@($1)/p_$1/gs;
		}
		$fct_detail{code}=~ s/\@([^\s]+)/l_$1/gip;
		

		# Move the DECLARE statement from code to the declare section.
		$fct_detail{declare} = '';
		while ($fct_detail{code} =~ s/DECLARE\s+([^;]+;)//is) {
				$fct_detail{declare} .= "$1\n";
		}
		# Now convert types
		if ($fct_detail{args}) {
			$fct_detail{args} = replace_sql_type($fct_detail{args}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type}, %{ $self->{data_type} });
		}
		if ($fct_detail{declare}) {
			$fct_detail{declare} = replace_sql_type($fct_detail{declare}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type}, %{ $self->{data_type} });
		}
		if ($fct_detail{code}) {
			$fct_detail{code} = replace_sql_type($fct_detail{code}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type}, %{ $self->{data_type} });
		}

		# Sometime variable used in FOR ... IN SELECT loop is not declared
		# Append its RECORD declaration in the DECLARE section.
		my $tmp_code = $fct_detail{code};
		while ($tmp_code =~ s/\bFOR\s+([^\s]+)\s+IN(.*?)LOOP//is) {
			my $varname = $1;
			my $clause = $2;
			if ($fct_detail{declare} !~ /\b$varname\s+/is) {
				chomp($fct_detail{declare});
				# When the cursor is refereing to a statement, declare
				# it as record otherwise it don't need to be replaced
				if ($clause =~ /\bSELECT\b/is) {
					$fct_detail{declare} .= "\n  $varname RECORD;\n";
				}
			}
		}

		# Set parameters for AUTONOMOUS TRANSACTION
		$fct_detail{args} =~ s/\s+/ /gs;
		push(@{$fct_detail{at_args}}, split(/\s*,\s*/, $fct_detail{args}));
		# Remove type parts to only get parameter's name
		push(@{$fct_detail{param_types}}, @{$fct_detail{at_args}});
		map { s/\s(IN|OUT|INOUT)\s/ /i; } @{$fct_detail{at_args}};
		map { s/^\(//; } @{$fct_detail{at_args}};
		map { s/^\s+//; } @{$fct_detail{at_args}};
		map { s/\s.*//; } @{$fct_detail{at_args}};
		map { s/\)$//; } @{$fct_detail{at_args}};
		@{$fct_detail{at_args}} = grep(/^.+$/, @{$fct_detail{at_args}});
		# Store type used in parameter list to lookup later for custom types
		map { s/^\(//; } @{$fct_detail{param_types}};
		map { s/\)$//; } @{$fct_detail{param_types}};
		map { s/\%ORA2PG_COMMENT\d+\%//gs; }  @{$fct_detail{param_types}};
		map { s/^\s*[^\s]+\s+(IN|OUT|INOUT)/$1/i; s/^((?:IN|OUT|INOUT)\s+[^\s]+)\s+[^\s]*$/$1/i; s/\(.*//; s/\s*\)\s*$//; s/\s+$//; } @{$fct_detail{param_types}};		

	} else {
                delete $fct_detail{func_ret_type};
                delete $fct_detail{declare};
                $fct_detail{code} = $code;
	}

	# Mark the function as having out parameters if any
	my @nout = $fct_detail{args} =~ /\bOUT\s+([^,\)]+)/igs;
	my @ninout = $fct_detail{args} =~ /\bINOUT\s+([^,\)]+)/igs;
	my $nbout = $#nout+1 + $#ninout+1;
	$fct_detail{inout} = 1 if ($nbout > 0);

	$fct_detail{args} =~ s/\s*(with\s+.*)/ /igs;
	$fct_detail{args} =~ s/\s*(AS.*)(\s*\%ORA2PG_COMMENT\d+\%)*\s/ /igs;
	$fct_detail{args} =~ s/\s+\s*(AS\s+.*)*\s/ /igs;
	if($fct_detail{args} =~ /^\s*\(/i){
 		$fct_detail{args} =~ s/\s*\((.*)\)(\s*\%ORA2PG_COMMENT\d+\%)*\s*$/$1$2/gs;
	}
	#($fct_detail{code}, $fct_detail{declare}) = replace_sqlserver_variables($self, $fct_detail{code}, $fct_detail{declare});	
	
	return %fct_detail;
}

=head2 _get_external_tables

TODO: add sub documentation

=cut

sub _get_external_tables
{
	# Only supported in 2016 and up
	my $self = shift;
	my %data = ();

	return %data;
}

=head2 _get_package_function_list

TODO: add sub documentation

=cut

sub _get_package_function_list
{
	# Packages do not exist in SQL Server
	my $self = shift;
	my $owner = shift;

	return;
}

=head2 _get_materialized_views

This function implements an SQL Server native materialized views information.

Returns a hash of view names with the SQL queries they are based on.

=cut

sub _get_materialized_views
{
	my $self = shift;
	# TODO: should be completed

	return;
}

=head2 _check_constraint TABLE OWNER

This function implements a check constraint information.

Returns a hash of lists of all column names defined as check constraints
for the specified table and constraint name.

=cut

sub _check_constraint
{
	my($self, $table, $owner) = @_;

	my $condition = '';
	$condition .= " and  TABLE_SCHEMA + '.' + TABLE_NAME='$table' " if ($table);
	# if ($owner) {
	# 	$condition .= "AND OWNER = '$owner' ";
	# } else {
	# 	$condition .= "AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	# }
	$condition .= $self->limit_to_objects('TABLE', 'TABLE_NAME') if (!$table);

# SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,DEFERRABLE,DEFERRED,R_OWNER,TABLE_NAME,OWNER,VALIDATED
# FROM $self->{prefix}_CONSTRAINTS
# WHERE CONSTRAINT_TYPE='C' $condition
# AND STATUS='ENABLED'

	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
select distinct TABLE_SCHEMA, TABLE_SCHEMA + '.' + TABLE_NAME
FROM   INFORMATION_SCHEMA.TABLE_CONSTRAINTS where 1 = 1 
$condition
ORDER BY 2
END

	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	my @table_list; # results from first query
	while (my @row = $sth->fetchrow) {
		push(@table_list,\@row);
		# push(@table_list,[ split /\|/, $row ]);
		# print "row(0)=$row[0], row(1)=$row[1]\n";
	}


	# We have to use sp_helpconstraint to get the CHECK definition
	# Had to use do while to be able to parse it and only the second result is of interest for us.
	

	foreach my $t (@table_list)
	{
		# print "0=" . @{$t}[0] . "\n";
		# print "1=" . @{$t}[1] . "\n";

		my $sql_hc = "exec sp_helpconstraint '$t->[1]'";
		# print "\tsql=[$sql_hc] \n\n";
		my $sth_hc = $self->{dbh}->prepare($sql_hc);
		$sth_hc->execute;
		my $n = 0;

		do {{
			while (my @row_hc = $sth_hc->fetchrow_array()) {
				next if (($n==0) or ($n>1));
				# print "\trow(0)=$row_hc[0] \n";
				# print "\trow(6)=$row_hc[6] \n";
				if ($row_hc[0] =~ 'CHECK') {
					# Prelimary rewrite of the CHECK condition
					$row_hc[6] =~ s/\(([+-]?([0-9]*[.])?[0-9]+)\)/$1/igs;
					$row_hc[6] =~ s/like/~/igs if $row_hc[6] =~ /like\s*'\[/igs;    # we rewrite "like '[xxx]'" as postgres does not accept as much within the "like" clause
					$data{@{$t}[1]}{constraint}{$row_hc[1]}{condition} = $row_hc[6] ;
				}
			}
			$n++;
		}} while ($sth_hc->{odbc_more_results}) 

	}

	return %data;
}

=head2 _get_types

This function retruns an SQL Server custom types information.

Returns a hash of all custome type names with their type.

=cut

sub _get_types
{
	my ($self, $dbh, $name) = @_;

	my @types = ();
    my $str = "select Name+'( '+case when is_table_type = 1 then 'Table Type' else 'Data Type' end +' )', SCHEMA_NAME(schema_id) as OWNER,user_type_id,Name   from sys.types where is_user_defined = 1";
    $str .= " AND name='$name'" if ($name);	
	if ($self->{schema}) {
		$str .= " AND SCHEMA_NAME(schema_id) ='$self->{schema}' ";
	}
	if (!$name) {
		$str .= $self->limit_to_objects('TYPE', 'OBJECT_NAME');
	} else {
		@{$self->{query_bind_params}} = ();
	}
	$str .= " ORDER BY Name";

	my $sth = $dbh->prepare($str) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch) {
		my %tmp = ();
		$tmp{name} = $row->[0];
		$tmp{owner} = $row->[1];
		$tmp{pos} = $row->[2];
		push(@types, \%tmp);
	}
	$sth->finish();

	return \@types;	
}
=head2 sqlserver_to_plpgsql

This function turns a SQL Server function code into a PLPGSQL code

=cut

sub sqlserver_to_plpgsql
{
	# TODO: uuid_generate_v4 needs a CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        my ($class, $str) = @_;

	# $str =~ s/IF\s*OBJECT_ID\s*(\w+)\s*IS\s*\NOT\s*\NULL//igs;
	# $str =~ s/IF\s*OBJECT_ID//igs;

	# $str =~ s/SET\s*ANSI_NULLS\s*ON//igs;
	# $str =~ s/^GO$//igs;
	
	$str =~ s/getdate\s*\(\s*\)/CURRENT_TIMESTAMP/igs;
	$str =~ s/newid\s*\(\s*\)/uuid_generate_v4()/igs;

	# TODO: remove [] for identifiers, scope might be too wide.
	$str =~ s/\[(\w+)\]/$1/igs;
	$str =~ s/\[([a-zA-Z0-9.]+)\]/"$1"/ig;
	####
	# Replace some function with their PostgreSQL syntax
	####

	# Math related functions
	
	# Misc functions
	
	# Date/time related functions
	# dateadd: https://docs.microsoft.com/fr-fr/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-2017
	#https://www.postgresql.org/docs/9.0/functions-datetime.html
	$str =~ s/dateadd\s*\(\s*(yy|yyyy|year)\s*,\s*(\w+)\s*,\s*([^\(\),]+)\s*\)/date ($3) + interval '$2 year'/igs;
	$str =~ s/dateadd\s*\(\s*(yy|yyyy|year)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,\s*([^\(\),]+)\s*\)/date ($4) + interval '$2 year'/igs;
	$str =~ s/dateadd\s*\(\s*(m|mm|month)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,\s*([^\(\),]+)\s*\)/date ($4) + interval '$2 month'/igs;
	$str =~ s/dateadd\s*\(\s*(d|dd|day)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,\s*([^\(\),]+)\s*\)/date ($4) + interval '$2 day'/igs;
	$str =~ s/dateadd\s*\(\s*(hh|hour)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,\s*([^\(\),]+)\s*\)/date ($4) + interval '$2 hour'/igs;
	$str =~ s/dateadd\s*\(\s*(n|mi|minute)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,\s*([^\(\),]+)\s*\)/date ($4) + interval '$2 minute'/igs;
	$str =~ s/dateadd\s*\(\s*(s|ss|second)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,\s*([^\(\),]+)\s*\)/date ($4) + interval '$2 second'/igs;
	$str =~ s/date\s*CURRENT_TIMESTAMP/CURRENT_TIMESTAMP/igs;  # maybe too wide ?
	
	$str =~ s/dateadd\s*\(\s*(yy|yyyy|year)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,(\s*(@?\w+)\s*|\s*CONVERT\(\s*[^,]+\s*,\s*[^,]+\s*,\s*[^\(\),]+\s*\))\)/date $4 + interval '$2 year'/mi;
	$str =~ s/dateadd\s*\(\s*(m|mm|month)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,(\s*(@?\w+)\s*|\s*CONVERT\(\s*[^,]+\s*,\s*[^,]+\s*,\s*[^\(\),]+\s*\))\)/date $4 + interval '$2 month'/mi;
	$str =~ s/dateadd\s*\(\s*(d|dd|day)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,(\s*(@?\w+)\s*|\s*CONVERT\(\s*[^,]+\s*,\s*[^,]+\s*,\s*[^\(\),]+\s*\))\)/date $4 + interval '$2 day'/mi;
	$str =~ s/dateadd\s*\(\s*(hh|hour)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,(\s*(@?\w+)\s*|\s*CONVERT\(\s*[^,]+\s*,\s*[^,]+\s*,\s*[^\(\),]+\s*\))\)/date $4 + interval '$2 hour'/mi;
	$str =~ s/dateadd\s*\(\s*(n|mi|minute)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,(\s*(@?\w+)\s*|\s*CONVERT\(\s*[^,]+\s*,\s*[^,]+\s*,\s*[^\(\),]+\s*\))\)/date $4 + interval '$2 minute'/mi;
	$str =~ s/dateadd\s*\(\s*(s|ss|second)\s*,\s*([+-]?([0-9]*[.])?[0-9]+)\s*,(\s*(@?\w+)\s*|\s*CONVERT\(\s*[^,]+\s*,\s*[^,]+\s*,\s*[^\(\),]+\s*\))\)/date $4 + interval '$2 second'/mi;

	#LEN FUNCTION TO LENGTH FUNCTION ex : len('sds') -> Length('sds')
	$str =~ s/len\s*\(\s*([^\(\),]+)\s*\)/LENGTH($1)/igs;

	#DATENAME
	$str =~ s/datename\s*\(\s*(yy|yyyy|year)\s*,\s*([^\(\),]+)\s*\)/to_char($2, 'YYYY')/igs;	
	$str =~ s/datename\s*\(\s*(m|mm|month)\s*,\s*([^\(\),]+)\s*\)/to_char($2, 'month')/igs;
	$str =~ s/datename\s*\(\s*(week|ww|wk)\s*,\s*([^\(\),]+)\s*\)/to_char($2, 'ww')/igs;		
	$str =~ s/datename\s*\(\s*(d|dd|day)\s*,\s*([^\(\),]+)\s*\)/to_char($2, 'DD')/igs;
	$str =~ s/datename\s*\(\s*(weekday|dw|w)\s*,\s*([^\(\),]+)\s*\)/to_char($2, 'day')/igs;	
	$str =~ s/datename\s*\(\s*(n|mi|minute)\s*,\s*([^\(\),]+)\s*\)/to_char($2, 'minute')/igs;
	$str =~ s/datename\s*\(\s*(s|ss|second)\s*,\s*([^\(\),]+)\s*\)/to_char($2, 'second')/igs;
    $str =~ s/datename\s*\(\s*(dayofyear)\s*,\s*([^\(\),]+)\s*\)/to_char($2, 'DDD')/igs;
	$str =~ s/datename\s*\(\s*(quarter|qq|q)\s*,\s*([^\(\),]+)\s*\)/to_char($2, 'q')/igs;

	#day
	$str =~ s/day\s*\(\s*([^\(\),]+)\s*\)/to_char($1,'dd')/igs;

	#Month
	$str =~ s/Month\s*\(\s*([^\(\),]+)\s*\)/to_char($1,'MM')/igs;
	$str =~ s/YEAR\s*\(\s*([^\(\),]+)\s*\)/to_char($1,'YYYY')/igs;

	#datediff
	$str =~ s/DATEDIFF\s*\(\s*(yy|yyyy|year)\s*,\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('year', $3::date) - DATE_PART('year', $2::date)/igs;
	$str =~ s/DATEDIFF\s*\(\s*(m|mm|month)\s*,\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/(DATE_PART('year', $3::date) - DATE_PART('year', $2::date))* 12 + (DATE_PART('month', $3::date) - DATE_PART('month', $2::date))/igs;
	$str =~ s/DATEDIFF\s*\(\s*(d|dd|day)\s*,\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('day', $3::timestamp - $2::timestamp)/igs;
	# $str =~ s/DATEDIFF\s*\(\s*(week|ww|wk)\s*,\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/TRUNC(DATE_PART('day', $3::timestamp - $2::timestamp)7)/igs;
	$str =~ s/DATEDIFF\s*\(\s*(hh|hour)\s*,\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('day', $3::timestamp - $2::timestamp) * 24 + DATE_PART('hour', $3::timestamp - $2::timestamp)/igs;
	
	#datepart
	$str =~ s/datepart\s*\(\s*(year|yy|yyy)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('year', + TIMESTAMP + $2)/igs;
	$str =~ s/datepart\s*\(\s*(m|mm|month)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('month', + TIMESTAMP + $2)/igs;
	$str =~ s/datepart\s*\(\s*(d|dd|day)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('day', + TIMESTAMP + $2)/igs;
	$str =~ s/datepart\s*\(\s*(hh|hour)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('hour', + TIMESTAMP + $2)/igs;
	$str =~ s/datepart\s*\(\s*(n|mi|minute)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('minute', + TIMESTAMP + $2)/igs;
	$str =~ s/datepart\s*\(\s*(s|ss|second)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('second', + TIMESTAMP + $2)/igs;
	$str =~ s/datepart\s*\(\s*(qq|q|quarter)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('quarter', + TIMESTAMP + $2)/igs;
	$str =~ s/datepart\s*\(\s*(dw|w|weekday )\s*,\s*([^\(\),]+)\s*\)/DATE_PART('dow', + TIMESTAMP + $2)/igs;
    $str =~ s/datepart\s*\(\s*(dy|y|dayofyear)\s*,\s*([^\(\),]+)\s*\)/DATE_PART('doy', + TIMESTAMP + $2)/igs;
	
	#Replicate

	$str =~ s/Replicate\s*\(\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/repeat($1,$2)/igs; 

	#char
	$str =~ s/char\s*\(\s*([^\(\),]+)\s*\)/chr($1)/igs; 

	#Charindex
	$str =~ s/CHARINDEX\s*\(\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/strpos($2,$1)/igs; 

	#ISNUll
	$str =~ s/ISNUll\s*\(\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/COALESCE($1,$2)/igs; 

	#STUFF
	$str =~ s/STUFF\s*\(\s*([^\(\),]+)\s*,\s*(@?\w+)\s*,\s*(@?\w+)\s*,\s*([^\(\),]+)\s*\)/overlay($1 placing $4 from $2 for $3)/igs;

	#space
	$str =~ s/space\s*\(\s*(\s*([^\(\),]+)\s*)\)/REPEAT(' ',$1)/igs;

	#DATEFROMPARTS
	$str =~ s/DATEFROMPARTS\s*\(\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*,([^\(\),]+)\s*\)/make_date($1,$2,$3)/igs;
	
	#date conversion
	$str = convert_function($str);
	
	#Exception handling
	$str =~ s/\s+BEGIN\s*TRY/\r\n\tBEGIN/igs;
	$str =~ s/\s+END\s*TRY/\r\n/igs;
	$str =~ s/\s+BEGIN\s*CATCH/\r\n\tEXCEPTION/igs;
	$str =~ s/\s+END\s*CATCH/\r\n\tEND/igs;
	
	# SQL functions
	# TODO: outer apply / cross apply

	# TODO: alias can be written with "myalias = mycolumn", instead of "mycolumn as myalias"

	# Remove @ from variables and rewrite SET assignement in QUERY mode
	if ($class->{type} eq 'QUERY') {
		$str =~ s/\@([^\s]+)\b/$1/gs;
		$str =~ s/:=/=/gs;
	}
	return $str;
}

sub convert_function
{
	my ($str) = @_;

	
    my $regex = qr/\bCONVERT\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/mi;
	while($str =~ /$regex/gip)
	{
		my $data_type = $1;
        my $field = $2;
		my $format = $3;

		if($data_type =~/datetime/mi)
		{
			#Convert(datetime,'99990101',112)  to to_timestamp('99990101','yyyyddmm')
			$str =~ s/\bCONVERT\s*\(\s*$data_type\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/to_timestamp($1,'$SQL_Date_format{$2}')/igs;
		}
		else
		{
			#Convert(varchar,'99990101',112)  to to_char('99990101','yyyyddmm')
			$str =~ s/\bCONVERT\s*\(\s*$data_type\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/to_char($1,'$SQL_Date_format{$2}')/igs;          
	
		}
	}

	#Convert(int,10) 
	$str =~ s/\bCONVERT\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*\)/ CAST ($2 AS $1)/igs;   


	return $str;
}

1;

__END__

