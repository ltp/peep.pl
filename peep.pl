#!/usr/bin/perl

use strict;

use Switch;
use threads;
use threads::shared;
use Thread::Semaphore;
use Thread::Queue;
use IPC::Open3;
use Net::SSH qw(sshopen2);
use Time::HiRes qw(usleep);

my $mplayer 	= '/usr/bin/mplayer';
my $threads	= 20;
my $thread_to	= 20;
my %mplayer_threads;
my %mplayer_stat: shared;
my %mplayer_alive: shared;
my %monitor_threads;
my $parsed	: shared;
my $processed	: shared;
my $thread;
my @thread;
my $sem		: shared = 1;
my $mplayer_queue= Thread::Queue->new();

my $monitor	= {
			localhost_http	=> {
						hostname	=> 'localhost',
						username	=> 'username',
						command		=> 'tail -n 0 -f /var/log/apache2/access.log',
						parser		=> 'apache_clf'
					}
		#	webserver	=> {
		#				hostname	=> 'webserver.test.com',
		#				username	=> 'username',
		#				command		=> 'tail -f /var/log/httpd/access_log',
		#				parser		=> 'apache_clf'
		#			}
		};

my $parser	= {
			apache_clf	=> 	\&apache_clf_parser
		};

for(my $i=$threads;$i>0;$i--) {
	my $thread 			= threads->create('mplayer_thread') or sub {log_err('Failed to create a mplayer_thread'); next};
	my $tid				= $thread->tid();
	$mplayer_threads{$tid}{thread}	= $thread;
	$mplayer_threads{$tid}{ctime}	= time;
	$mplayer_stat{$tid}		= time;
	$mplayer_alive{$tid}		= 1;
}

foreach my $tmonitor (keys %{$monitor}) {
	my $tparser = $monitor->{$tmonitor}->{parser};

	unless (exists $parser->{$tparser}) {
		log_err("Specified parser type \"$tparser\" is unknown"); 
		next;
	}

	my $thread			= threads->create('parser_thread', $tmonitor) or sub {log_err("Failed to create a monitor thread of type $tmonitor"); next};
	my $tid				= $thread->tid();
	$monitor_threads{$tid}{thread}	= $thread;
	$monitor_threads{$tid}{ctime}	= time;
	$monitor_threads{$tid}{type}	= $tmonitor;
}

watchdog_thread();

sub watchdog_thread {
	while (sleep 1) {
		log_info('mplayer_queue length is '.$mplayer_queue->pending().". parsed: $parsed . processed: $processed");
		foreach my $tid (keys %mplayer_threads) {
			my $thread = $mplayer_threads{$tid}{thread};
			unless ($thread->is_running()) {
				if ($thread->is_joinable()) {
					$thread->join();
					log_info("joining mplayer_thread $tid");
				}
				else {
					log_err("mplayer_thread $tid died. detaching.");
					$thread->detach();
				}
			}
		}
		foreach my $tid (keys %monitor_threads) {
			my $thread = $monitor_threads{$tid}{thread};
			unless ($thread->is_running()) {
				log_err('monitor_thread '.$thread->tid().' is not running');
				if ($thread->is_joinable()) {
					$thread->join();
					log_info("joining monitor_thread $tid");
				}
				else {
					log_err("monitor_thread $tid died. detaching.");
					$thread->detach();
				}
			}
		}
	}
}

sub parser_thread {
	$SIG{'KILL'}= sub { return };
	$SIG{'TERM'}= sub { return };

	my $instance	= shift;
	my $hostname	= $monitor->{$instance}->{hostname};
	my $user	= $monitor->{$instance}->{username};
	my $command	= $monitor->{$instance}->{command};
	my $_parser	= $monitor->{$instance}->{parser};
	my $parser	= $parser->{$_parser};
	my($rd,$wr);

	if($hostname eq 'localhost') {
		log_info("attempting local log command $command");
		open(RD, "$command|") or log_err("Couldn't instantiate monitor \"$monitor\" (localhost: $command)");
	}
	else {
		print "quit\n";exit;
		log_info("attempting sshopen2($user\@$hostname, *RD, $wr, \"$command\")");
		sshopen2("$user\@$hostname", \*RD, $wr, "$command") or log_err("Couldn't instantiate monitor \"$monitor\" ($user\@$hostname:$command parser:$parser)");
	}

	while(<RD>) {
		$parser->($_);
		$parsed++;
	}

	log_err('exiting parser_thread');
}

sub mplayer_thread {
	$SIG{'KILL'}	= sub { return 0 };
	$SIG{'TERM'}	= sub { return 0 };
	$SIG{'ALRM'}	= sub { return 0 };

	my($rd,$wr,$err);
	my $mplayer = open3($wr, $rd, $err, 'mplayer -v -idle -slave -quiet sounds/irds001.mp3');
	usleep(rand(500_000));
	my $tid	= threads->tid();

	while($sem) {
		if(defined(my $job = $mplayer_queue->dequeue_nb())) {
			print $wr $job;
			$processed++;
			while(<$rd>) {
				last if /EOF code: 1/;
			}
		}
	}
	log_info("mplayer_thread $tid exiting");
	print $wr "quit\n";
	waitpid($mplayer, 0);
}

sub apache_clf_parser {
	my $_		= shift;
	my $file;
	my($stat,$size)	= (split)[8,9];
	my @ok		= qw(birds007.mp3 birds008.mp3 birds014.mp3 birds022.mp3);
	my $sstat	= substr $stat, 0, 1;

	switch ($sstat) {
		case 2	{$file = $ok[rand(@ok)]} 
		case 3	{$file = $ok[rand(@ok)]}
		case 4	{$file = $ok[rand(@ok)]} 
		case 5	{$file = 'birds009.mp3'}
	}

	switch ($stat) {
		case 401	{$file = 'birds005.mp3'}
		case 403	{$file = 'birds005.mp3'}
		case 404	{$file = 'birds028.mp3'}
	}
	
	$file or return;
	$file	= "loadfile sounds/$file\n";
	$mplayer_queue->enqueue($file);
}

sub log_err {
	my $message	= shift;
	_log('ERROR', $message);
}

sub log_warn {
	my $message 	= shift;
	_log('WARNING', $message);
}

sub log_info {
	my $message 	= shift;
	_log('INFO', $message);
}

sub _log {
	my($level, $message) = @_;
	print '[', scalar localtime, "] [$0 ($$)] [$level] - $message\n";
}

END {
	$sem = 0;

	foreach my $tid (keys %mplayer_threads) {
		my $thread = $mplayer_threads{$tid}{thread};
		$thread->kill('KILL');
		$thread->join();
	}

	foreach my $tid (keys %monitor_threads) {
		my $thread = $monitor_threads{$tid}{thread};
		$thread->kill('KILL');
		$thread->join();
	}

}
