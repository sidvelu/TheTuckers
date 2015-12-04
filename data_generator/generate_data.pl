#!/usr/bin/env perl
use strict;
use warnings;
use feature 'say';

sub stars {
	return (int(rand(9)) + 2) / 2.0
}

sub gri { #generate random int
	return (int(rand(100)));
}

sub grd { #generate random double
	return (rand(100));
}

sub grs { #generate random string
	my ($length) = @_;
	my @chars = ("A".."Z", "a".."z");
	my $string;
	$string .= $chars[rand @chars] for 1..$length;
	return $string;
}

sub generate_json {
	my ($type) = @_;
	if ($type eq "Review") {
		return '{"type":"review","business_id":"'.grs(22).'","user_id":"'.grs(22).'","stars":'.stars().',"text":"'.grs(80).'","date":"'.grs(8).'","votes":{}}';
	} elsif ($type eq "Business") {
		return '{"type":"business","business_id":"'.grs(22).'","name":"'.grs(10).'","neighborhoods":["'.grs(4).'"],"full_address":"'.grs(25).'","city":"'.grs(8).'","state":"'.grs(4).'","latitude":"'.grd().'","longitude":"'.grd().'","stars":'.stars().',"review_count":"'.gri().'","categories":["'.grs(2).'"],"open":"True","hours":{},"attributes":{}}'
	} else {
		say "Invalid type";
		exit;
	}
}

sub print_file {
	my ($type, $output, $number) = @_;
	for my $i (0..$number) {
		my $record = generate_json($type);
		say $output $record;
	}
	
}

if (@ARGV < 2) {
	say "generate_reviews.pl <type> <# entries>";
	say "Types: Review, Business";
	exit;
}

my $RecordType = $ARGV[0];
my $NumberRecords = $ARGV[1];
my $Filename;

if ($RecordType eq "Review") {
	$Filename = "reviews.json";
} elsif ($RecordType eq "Business") {
	$Filename = "businesses.json";
} else {
	say "Invalid type.";
	exit;
}

open(my $out, ">", $Filename) or die "Can't open output.txt: $!";
print_file($RecordType, $out, $NumberRecords);

