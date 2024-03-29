package Scraperwiki::DumpTruck;

=head1 NAME

Scraperwiki::DumpTruck - Relaxing interface to SQLite

=head1 SYNOPSIS

  my $dt = new Scraperwiki::DumpTruck;

  $dt->insert({Hello => 'World'});
  $dt->create_index(['Hello'], 'dumptruck');
  $dt->upsert({Hello => 'World', Yolo => 8086});
  my $data = $dt->dump;

  $dt->insert([
      {Hello => 'World'},
      {Hello => 'Hell', Structured => {
          key => value,
          array => [ 1, 2, 3, {} ],
      }}], 'table2');
  my $data2 = $dt->dump('table2');
  $dt->drop('table2');
  $dt->execute('SELECT 666');

  my @columns = $dt->column_names();

  $dt->save_var('number_of_the_beast', 666);
  my $number_of_the_beast = $dt->get_var('number_of_the_beast');

=head1 DESCRIPTION

This is a simple document-oriented interface to a SQLite database, modelled
after Scraperwiki's Python C<dumptruck> module. It allows for easy (and maybe
inefficient) storage and retrieval of structured data to and from a database
without interfacing with SQL.

=cut

use strict;
use warnings;

use DBI;
use B;
use JSON;
require DBD::SQLite;

sub get_column_type
{
	my $v = shift;

	return unless defined $v;

	# A reference?
	my $ref = ref $v;
	if ($ref) {
		return 'json text' if $ref eq 'ARRAY' or $ref eq 'HASH';
		# TODO: blessings into some magic package names to force a type?
		# TODO: What's the most canonical package to describe datetime?
	}

	# A scalar.
	my $obj = B::svref_2object (\$v);
	my $flags = $obj->FLAGS;

	# Could here be a better way to detect a boolean?
	if (($flags & (B::SVf_IOK | B::SVf_NOK | B::SVf_POK))
		== (B::SVf_IOK | B::SVf_NOK | B::SVf_POK))
	{
		return 'bool'
			if ($obj->IV == 0 && $obj->NV == 0 && $obj->PV eq '')
			or ($obj->IV == 1 && $obj->NV == 1 && $obj->PV eq '1');
	}

	return 'text' if $flags & B::SVf_POK;
	return 'real' if $flags & B::SVf_NOK;
	return 'integer' if $flags & B::SVf_IOK;

	return 'text';
}

sub convert
{
	my $data = shift;
	my @retval;

	foreach my $row (ref $data eq 'ARRAY' ? @$data : ($data)) {
		push @retval, [ map { [ $_ => $row->{$_} ] } sort keys %$row ];
	}

	return \@retval;
}

sub simplify
{
	my $text = shift;
	$text =~ s/[^a-zA-Z0-9]//g;
	return $text;
}

=head1 METHODS

=over 4

=item B<new> ([params])

Initialize the database handle. Accepts optional hash with parameters:

=over 8

=item B<dbname> (Default: C<dumptruck.db>)

The database file.

=item B<vars_table> (Default: C<_dumptruckvars>)

Name of the variables table.

=item B<auto_commit> (Default: C<1>)

Enable automatic commit.

=back

=cut

sub new
{
	my $class = shift;
	my $self = shift || {};

	$self->{dbname} ||= 'dumptruck.db';
	$self->{vars_table} ||= '_dumptruckvars';
	$self->{auto_commit} = 1
		unless exists $self->{auto_commit};

	$self->{dbh} = DBI->connect("dbi:SQLite:$self->{dbname}","","", {
		AutoCommit => $self->{auto_commit},
		RaiseError => 1, PrintError => 0 })
		or die "Could get a database handle: $!";
	$self->{dbh}{sqlite_unicode} = 1;

	return bless $self, $class;
}

=item B<column_names> ([table_name])

Return a list of names of all columns in given table, or table C<dumptruck>.

=cut

sub column_names
{
	my $self = shift;
	my $table_name = shift || 'dumptruck';

	$self->execute (sprintf 'PRAGMA table_info(%s)',
		$self->{dbh}->quote ($table_name))
}

sub _check_or_create_vars_table
{
	my $self = shift;

	$self->execute (sprintf 'CREATE TABLE IF NOT EXISTS %s '.
		'(`key` text PRIMARY KEY, `value` blob, `type` text)',
		$self->{dbh}->quote ($self->{vars_table}));
}

=item B<execute> (sql, [params])

Run a raw SQL statement and get structured output. Optional parameters for C<?>
placeholders can be specified.

=cut

sub execute
{
	my $self = shift;
	my $sql = shift;
	my @params = @_;
	my @retval;

	warn "Executing statement: '$sql'" if $self->{debug};
	my $sth = $self->{dbh}->prepare ($sql);
	$sth->execute (@params);

	return [] unless $sth->{NUM_OF_FIELDS};

	while (my $row = $sth->fetch) {
		my $types = $sth->{TYPE};
		my $names = $sth->{NAME_lc};
		push @retval, {};

		foreach (0..$#$row) {
			my $data = $row->[$_];
			$data = decode_json ($data) if $types->[$_] eq 'json text';
			$retval[$#retval]->{$names->[$_]} = $data;
		}
	};

	return \@retval;
}

=item B<commit> ()

Commit outstanding transaction. Useful when C<auto_commit> is off.

=cut

sub commit
{
	my $self = shift;

	$self->{dbh}->commit;
}

=item B<close> ()

Close the database handle. You should not need to call this explicitly.

=cut

sub close
{
	my $self = shift;

	$self->{dbh}->disconnect;
	$self->{dbh} = undef;
}

=item B<create_index> (columns, table_name, [if_not_exists], [unique])

Create an optionally unique index on columns in a given table. Can be told
to do nothing if the index already exists.

=cut

sub create_index
{
	my $self = shift;
	my $columns = shift;
	my $table_name = shift;
	my $if_not_exists = shift;
	$if_not_exists = (not defined $if_not_exists or $if_not_exists)
		? 'IF NOT EXISTS' : '';
	my $unique = (shift) ? 'UNIQUE' : '';

	my $index_name = join '_', (simplify ($table_name),
		map { simplify ($_) } @$columns);

	$self->execute (sprintf 'CREATE %s INDEX %s %s ON %s (%s)',
		$unique, $if_not_exists, $index_name,
		$self->{dbh}->quote ($table_name),
		join (',', map { $self->{dbh}->quote ($_) } @$columns));
}

sub _check_and_add_columns
{
	my $self = shift;
	my $table_name = shift;
	my $row = shift;

	foreach (@$row) {
		my ($k, $v) = @$_;
		eval { $self->execute (sprintf 'ALTER TABLE %s ADD COLUMN %s %s',
			$self->{dbh}->quote ($table_name),
			$self->{dbh}->quote ($k), get_column_type ($v)) };
		die if $@ and not $@ =~ /duplicate column name/;
	}
}

=item B<create_table> (data, table_name, [error_if_exists])

Create a table and optionally error out if it already exists. The data
structure will be based on data, though no data will be inserted.

=cut

sub create_table
{
	my $self = shift;
	my $data = shift;
	my $table_name = shift or die 'Need table name';
	my $error_if_exists = shift;

	# Get ordered key-value pairs
	my $converted_data = convert ($data);
	die 'No data passed' unless $converted_data and $converted_data->[0];

	# Find first non-null column
	my $startdata = $converted_data->[0];
	my ($k, $v);
	foreach (@$startdata) {
		($k, $v) = @$_;
		last if defined $v;
	}

	# None.
	return unless $k and $v;

	# Create the table with the first column
	my $if_not_exists = 'IF NOT EXISTS' unless $error_if_exists;
	$self->execute (sprintf 'CREATE TABLE %s %s (%s %s)',
		$if_not_exists, $self->{dbh}->quote ($table_name),
		$self->{dbh}->quote ($k), get_column_type ($v));

	# Add other rows
	foreach (@$converted_data) {
		$self->_check_and_add_columns ($table_name, $_);
	}
}

=item B<insert> (data, [table_name], [upsert])

Insert (and optionally replace) data into a given table or C<dumptruck>.
Creates the table with proper structure if it does not exist already.

=cut

sub insert
{
	my $self = shift;
	my $data = shift;
	my $table_name = shift || 'dumptruck';
	my $upsert = shift;

	# Override existing entries
	my $upserttext = ($upsert ? 'OR REPLACE' : '');

	# Ensure the table itself exists
	$self->create_table ($data, $table_name);

	# Learn about the types of already existing fields
	my %column_types = map { lc($_->{name}) => $_->{type} }
		@{$self->column_names ($table_name)};

	# Get ordered key-value pairs
	my $converted_data = convert ($data);
	die 'No data passed' unless $converted_data and $converted_data->[0];

	# Add other rows
	my @rowids;
	foreach (@$converted_data) {
		$self->_check_and_add_columns ($table_name, $_);

		my (@keys, @values);
		foreach my $cols (@$_) {
			my ($key, $value) = @$cols;

			# Learn about the type and possibly do a conversion
			my $type = $column_types{lc($key)} or get_column_type ($value);
			$value = encode_json ($value) if $type eq 'json text';

			push @keys, $key;
			push @values, $value;
		}

		if (@keys) {
			my $question_marks = join ',', map { '?' } 1..@keys;
			$self->execute (sprintf ('INSERT %s INTO %s (%s) VALUES (%s)',
				$upserttext, $self->{dbh}->quote ($table_name),
				join (',', @keys),
				$question_marks), @values);
		} else {
			$self->execute (sprintf 'INSERT %s INTO %s DEFAULT VALUES',
				$upserttext, $self->{dbh}->quote ($table_name));
		}

		push @rowids, $self->execute ('SELECT last_insert_rowid()')
			->[0]{'last_insert_rowid()'};
	}
	return (ref $data eq 'HASH' and $data->{keys}) ? $rowids[0] : @rowids;
}

=item B<upsert> (data, [table_name])

Replace data into a given table or C<dumptruck>. Creates the table with proper
structure if it does not exist already.

Equivalent to calling C<insert> with C<upsert> parameter set to C<1>.

=cut

sub upsert
{
	my $self = shift;
	my $data = shift;
	my $table_name = shift;

	$self->insert ($data, $table_name, 1);
}

=item B<get_var> (key)

Retrieve a saved value for given key from the variable database.

=cut

sub get_var
{
	my $self = shift;
	my $k = shift;

	my $data = $self->execute(sprintf ('SELECT * FROM %s WHERE `key` = ?',
		$self->{dbh}->quote ($self->{vars_table})), $k);
	return unless $data and $data->[0];
	return $data->[0]{value};
}

=item B<save_var> (key, value)

Insert a value for given key into the variable database.

=cut

sub save_var
{
	my $self = shift;
	my $k = shift;
	my $v = shift;

	$self->_check_or_create_vars_table;

	$self->execute(sprintf ('INSERT OR REPLACE INTO %s '.
		'(`key`, `type`, `value`) VALUES (?, ?, ?)',
		$self->{dbh}->quote ($self->{vars_table})),
		$k, get_column_type ($v), $v);
}

=item B<tables> ()

Returns a list of names of all tables in the database.

=cut

sub tables
{
	my $self = shift;

	map { $_->{name} } @{$self->execute
		('SELECT name FROM sqlite_master WHERE TYPE="table"')};
}

=item B<dump> ([table_name])

Returns all data from the given table or C<dumptduck> nicely structured.

=cut

sub dump
{
	my $self = shift;
	my $table_name = shift || 'dumptruck';

	$self->execute (sprintf 'SELECT * FROM %s',
		$self->{dbh}->quote ($table_name))
}

=item B<drop> ([table_name])

Drop the given table or C<dumptruck>.

=cut

sub drop
{
	my $self = shift;
	my $table_name = shift || 'dumptruck';
	my $if_exists = shift;

	$self->execute (sprintf 'DROP TABLE %s %s',
		($if_exists ? 'IF EXISTS' : ''),
		$self->{dbh}->quote ($table_name))
}

=back

=head1 BUGS

Structured values won't work for variables.

=head1 SEE ALSO

=over

=item *

L<https://github.com/scraperwiki/dumptruck> - Python module this one is
heavily inspired by.

=item *

L<Scraperwiki> - Data Developer module documentation

=back

=head1 COPYRIGHT

This program is free software; you can redistribute it and/or modify it
under the same terms as Scraperwiki itself.

=head1 AUTHOR

Lubomir Rintel L<< <lkundrak@v3.sk> >>

=cut

1;
