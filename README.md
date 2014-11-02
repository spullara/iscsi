Support
=======

The underlying jSCSI library doesn't support the Mac initiators (GlobalSan and AttoTech) very well. They will sort of
work but crap out in the middle of large file transfers. As far as I can tell, open-iscsi on Linux works great with it.

Typical discovery command:

sudo iscsiadm --mode discoverydb --type sendtargets --portal 192.168.2.1:3260 --discover

Typical login command:

sudo iscsiadm -m node -T iqn.2014-11.com.sampullara:storage:fdbiscsi -p 192.168.2.1 --login

I've been using XFS as it doesn't fill the disk with junk like ext4 does upon format.

Mac Support
===========

Both the iSCSI initiators try to MODE SENSE and that fails at the beginning of the connection. Not sure if
that is related.
