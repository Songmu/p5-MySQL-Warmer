package MySQL::Warmer;
use 5.008001;
use strict;
use warnings;

our $VERSION = "0.01";

use DBIx::Inspector;

use Moo;

has dbh => (
    is      => 'ro',
    isa     => sub { shift->isa('DBI::db') },
    lazy    => 1,
    default => sub {
        require DBI;
        DBI->connect(@{ shift->dsn });
    },
);

has dsn => (
    is  => 'ro',
    isa => sub { ref $_[0] eq 'ARRAY' },
);

has _inspector => (
    is   => 'ro',
    lazy => 1,
    default => sub {
        DBIx::Inspector->new(dbh => shift->dbh)
    },
);

no Moo;

sub run {
    my $self = shift;

    my $inspector = $self->_inspector;
    for my $table ($inspector->tables) {
        my @table_pk = map { $_->name } $table->primary_key;

        my @selectee;
        for my $pk (@table_pk) {
            my $pk_column = $table->column($pk);
            my $data_type_name = uc $pk_column->type_name;
            if ($data_type_name =~ /(?:INT(?:EGER)?|FLOAT|DOUBLE|DECI(?:MAL)?)$/) {
                push @selectee, sprintf "SUM(%s)", $pk_column->name;
            }
            elsif ($data_type_name =~ /(?:DATE|TIME)/) {
                push @selectee, sprintf "SUM(UNIX_TIMESTAMP(%s))", $pk_column->name;
            }
            else {
                push @selectee, sprintf "SUM(LENGTH(%s))", $pk_column->name;
            }
        }

        my $query = sprintf 'SELECT %s FROM %s ORDER BY %s;',
            join(', ', @selectee), $table->name, join(', ', @table_pk);

        print "$query\n";
        $self->dbh->do($query);

        # my $indexes = $self->statistics_info(table => $table->name, schema => $table->schema);
    }

}

sub statistics_info {
    my ($self, %args) = @_;
    my $table     = $args{table};
    my $schema    = $args{schema};
    my $dbh       = $self->dbh;

    my $sql = <<'...';
SELECT
    index_name,
    column_name
FROM Information_schema.STATISTICS
WHERE
    table_schema = ? AND
    table_name   = ?
ORDER BY table_schema, table_name, seq_in_index;
...

    my $sth = $dbh->prepare($sql);
    $sth->execute($schema, $table);
    $sth->fetchall_arrayref(+{});
}

1;
__END__

=encoding utf-8

=head1 NAME

MySQL::Warmer - It's new $module

=head1 SYNOPSIS

    use MySQL::Warmer;

=head1 DESCRIPTION

MySQL::Warmer is ...

=head1 LICENSE

Copyright (C) Songmu.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Songmu E<lt>y.songmu@gmail.comE<gt>

=cut

