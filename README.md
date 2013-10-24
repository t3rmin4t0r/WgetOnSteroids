WgetOnSteroids
==============

This started out as a streaming tutorial for downloading files with wget -c onto HDFS.

But it quickly became apparent that it was vastly more efficient to rely on java's native URL input-streams.

The input format is actually a space separated file containing

`http://example.com/a.txt /subdir/a.txt`

And when run with

`hadoop jar target/*.jar -d /tmp/foo/ -u urls.txt -n 1`

which will download a.txt into /tmp/foo/subdir/a.txt

Ideally, I should have put deleteOnExit on a.txt and released that lock once the task was cleaning up, but this worked already, just didn't fail cleanly (or resume, from where it left off).
