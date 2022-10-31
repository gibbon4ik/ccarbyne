#!/usr/bin/env perl
use strict;
use warnings;
use utf8;
use locale;

use IO::Socket;
use Socket qw(INADDR_ANY);
use Time::HiRes;

my @pids;

sub END {
    if (@pids) {
        print "kill @pids\n";
        kill 'TERM', @pids;
    }
    exit();
}

# listen aggregated stat
my $recive = '127.0.0.1:2003';
# send metrics to
my $sendto = '127.0.0.1:2023';
my $interval = 30;
my $startport = 2100;
my $cagg = 10;

my @metrics = qw(
    level1
    level2.test1 level2.test2 level2.test3
    level3.level2.test1 level3.level2.test2 level3.level2.test3
);
#@metrics = qw(level1);

my ($graphitehost, $graphiteport) = split /:/, $sendto;
my $sock = _connect($graphitehost, $graphiteport);

my ($rhost, $rport) = split /:/, $recive;
my $rsock = IO::Socket::INET->new(LocalAddr => $rhost, LocalPort => $rport, Proto => 'udp') or die "socket: $@";
print "Awaiting UDP messages on port $rport\n";

my $recvpid = fork();
die "reciver fork() failed: $!" unless defined $recvpid;
unless ($recvpid) {
    my $newmsg;
    while ($rsock->recv($newmsg, 1420)) {
        my($port, $ipaddr) = sockaddr_in($rsock->peername);
        #$hishost = gethostbyaddr($ipaddr, AF_INET);
        print "time=",time(),"\n";
        print "$newmsg\n";
    }
    exit;
}
push @pids, $recvpid;

# run main graphite aggregator
my $agpid = fork();
die "aggregator fork() failed: $!" unless defined $agpid;
unless ($agpid) {
    exec("./ccarbyne -i $interval -w 10 -f -l $sendto -r $recive");
}
push @pids, $agpid;

my %sockets;
# run cascade aggregators
for (my $i=0; $i<$cagg; $i++) {
    my $port = $startport+$i;
    my $ip = "127.0.0.1";
    my $cagpid = fork();
    die "caggregator fork() failed: $!" unless defined $cagpid;
    unless ($cagpid) {
        exec("./ccarbyne -i $interval -w 2 -c -l $ip:$port -r $sendto");
    }
    push @pids, $cagpid;
    $sockets{$port} = _connect($ip, $port);
}

$SIG{INT} = \&killall;
$SIG{TERM} = \&killall;
my $sttime = int(time() / $interval) * $interval;
my $count = 0;
my $skip = 0;
while(1) {
    my $t = time() - $sttime;
    if ($t < $interval * 1) {
        my %bulk;
        for (@metrics) {
            $bulk{'count.'.$_} = 1;
        }
        _sendbulk($sock,\%bulk);
        my $port = int(rand($cagg))+$startport;
        _sendbulk($sockets{$port},\%bulk,'cascade');
        $count++;
    }
    if ($t >= $interval) {
        print "send count = $count\n";
        $count = 0;
        $sttime = int(time() / $interval) * $interval;
    }
    Time::HiRes::sleep(1e-4);
}

kill 'TERM', @pids;
wait();
exit(0);

sub killall {
    if (@pids) {
        print "kill @pids\n";
        kill 'TERM', @pids;
    }
    exit();
}

sub _connect {
    my ($host, $port) = @_;
    my $sock;
    eval {
        if ($host =~ /^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$/) {
            $host = inet_aton($host) or die "inet_aton: $!";
        } else {
            $host = gethostbyname($host) or die "gethostbyname: $!";
        }

        # create & connect socket
        my $proto = getprotobyname('udp');
        my $saddr = sockaddr_in($port, $host) or die "sockaddr_in: $!";
        socket($sock, PF_INET, SOCK_DGRAM, $proto) or die "socket: $!";
        bind($sock, pack_sockaddr_in($port+50000, INADDR_ANY)) or die "bind: $!";
        connect($sock, $saddr) or die "connect: $!";
    };

    die "no logging to $host:$port because of <$@>" if $@;
    return $sock;
}

sub _sendbulk {
    my ($sock, $bulk, $project) = @_;
    my ($data, $timestamp);

    $project ||= 'test';
    my $port = $graphiteport;

    return undef unless $sock;
    unless (getpeername($sock)) { # reconnect
        $sock = _connect($graphitehost, $port);
    }

    $timestamp = time();
    $data = '';

    my ($err) = 0;
    foreach my $key (keys %$bulk ) {
        my ($param) = $key;
        $param =~ s/([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)/$1_$2_$3_$4/g;

        my $t = sprintf("%s.%s %s %s\n", $project, $param, $bulk->{$key}, $timestamp);

        if (length($data) + length($t) > 1024 ) {
            send($sock, $data, 0) or $err++;
            $data = $t;
        } else {
            $data .= $t;
        }
    }
    if ($data) {
        send($sock, $data, 0) or $err++;
    }
    return $err == 0 ? 1 : undef;
}

