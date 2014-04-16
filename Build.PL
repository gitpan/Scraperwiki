use strict;
use warnings;

use Module::Build;

my $build = Module::Build->new(
	module_name	=> 'Scraperwiki',
	license		=> 'perl',
	dist_author	=> 'Lubomir Rintel <lkundrak@v3.sk>',
	dist_version	=> '0.002_001',
	requires	=> {
		'DBI'		=> 0,
		'DBD::SQLite'	=> 0,
		'LWP::Simple'	=> 0,
	},
	configure_requires => {
		'Module::Build'	=> 0,
	},
	build_requires => {
		'Test::More'	=> 0,
		'Test::Exception' => 0,
		'File::Temp'	=> 0,
	}
);

$build->create_build_script;
