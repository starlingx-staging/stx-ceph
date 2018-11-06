#!/usr/bin/python
#
# Copyright (c) 2016 Wind River Systems, Inc.
#
# SPDX-License-Identifier: Apache-2.0
#

import ast
import os
import os.path
import re
import subprocess
import sys


#########
# Utils #
#########

def command(arguments, **kwargs):
    """ Execute e command and capture stdout, stderr & return code """
    process = subprocess.Popen(
        arguments,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **kwargs)
    out, err = process.communicate()
    return out, err, process.returncode


def get_input(arg, valid_keys):
    """Convert the input to a dict and perform basic validation"""
    json_string = arg.replace("\\n", "\n")
    try:
        input_dict = ast.literal_eval(json_string)
        if not all(k in input_dict for k in valid_keys):
            return None
    except Exception:
        return None

    return input_dict


def get_partition_uuid(dev):
    output, _, _ = command(['blkid', dev])
    try:
        return re.search('PARTUUID=\"(.+?)\"', output).group(1)
    except AttributeError:
        return None


###########################################
# Manage Journal Disk Partitioning Scheme #
###########################################

DISK_BY_PARTUUID = "/dev/disk/by-partuuid/"

def is_partitioning_correct(disk_node, partition_sizes):
    """ Validate the existence and size of journal partitions"""

    # Check that partition table format is GPT
    output, _, _ = command(["udevadm", "settle", "-E", disk_node])
    output, _, _ = command(["parted", "-s", disk_node, "print"])
    if not re.search('Partition Table: gpt', output):
        print "Format of disk node %s is not GPT, zapping disk" % disk_node
        return False

    # Check each partition size
    partition_index = 1
    for size in partition_sizes:
        # Check that each partition size matches the one in input
        partition_node = disk_node + str(partition_index)
        output, _, _ = command(["udevadm", "settle", "-E", partition_node])
        cmd = ["parted", "-s", partition_node, "unit", "MiB", "print"]
        output, _, _ = command(cmd)

        regex = ("^Disk " + str(partition_node) + ":\\s*" +
                 str(size) + "[\\.0]*MiB")
        if not re.search(regex, output, re.MULTILINE):
            print ("Journal partition %(node)s size is not %(size)s, "
                   "zapping disk" % {"node": partition_node, "size": size})
            return False

        partition_index += 1

    output, _, _ = command(["udevadm", "settle", "-t", "10"])
    return True


def create_partitions(disk_node, partition_sizes):
    """ Recreate partitions """

    # After creating a new partition table on a device, Udev does not
    # always remove old symlinks (i.e. to previous partitions on that device).
    # Also, even if links are erased before zapping the disk, some of them will
    # be recreated even though there is no partition to back them!
    # Therefore, we have to remove the links AFTER we erase the partition table
    # DISK_BY_PARTUUID directory is not present at all if there are no
    # GPT partitions on the storage node so nothing to remove in this case
    links = []
    if os.path.isdir(DISK_BY_PARTUUID):
        links = [ os.path.join(DISK_BY_PARTUUID,l) for l in os.listdir(DISK_BY_PARTUUID)
                   if os.path.islink(os.path.join(DISK_BY_PARTUUID, l)) ]

    # Erase all partitions on current node by creating a new GPT table
    _, err, ret = command(["parted", "-s", disk_node, "mktable", "gpt"])
    if ret:
        print ("Error erasing partition table of %(node)s\n"
               "Return code: %(ret)s reason: %(reason)s" %
               {"node": disk_node, "ret": ret, "reason": err})
        exit(1)

    # Erase old symlinks
    for l in links:
        if disk_node in os.path.realpath(l):
            os.remove(l)

    # Create partitions in order
    used_space_mib = 1  # leave 1 MB at the beginning of the disk
    for size in partition_sizes:
        cmd = ['parted', '-s', disk_node, 'unit', 'mib',
               'mkpart', 'primary',
               str(used_space_mib), str(used_space_mib + size)]
        _, err, ret = command(cmd)
        parms = {"disk_node": disk_node,
                 "start": used_space_mib,
                 "end": used_space_mib + size,
                 "reason": err}
        print ("Created partition from start=%(start)s MiB to end=%(end)s MiB"
               " on %(disk_node)s" % parms)
        if ret:
            print ("Failed to create partition with "
                   "start=%(start)s, end=%(end)s "
                   "on %(disk_node)s reason: %(reason)s" % parms)
            exit(1)
        used_space_mib += size

###########################
# Manage Journal Location #
###########################

OSD_PATH = "/var/lib/ceph/osd/"


def mount_data_partition(data_node, osdid):
    """ Mount an OSD data partition and return the mounted path """
    mount_path = OSD_PATH + "ceph-" + str(osdid)
    output, _, _ = command(['mount'])
    regex = "^" + data_node + ".*" + mount_path
    if not re.search(regex, output, re.MULTILINE):
        cmd = ['mount', '-t', 'xfs', data_node, mount_path]
        _, _, ret = command(cmd)
        params = {"node": data_node, "path": mount_path}
        if ret:
            print "Failed to mount %(node)s to %(path), aborting" % params
            exit(1)
        else:
            print "Mounted %(node)s to %(path)s" % params
    return mount_path


def is_location_correct(path, journal_node, osdid):
    """ Check if location points to the correct device """
    cur_node = os.path.realpath(path + "/journal")
    if cur_node == journal_node:
        return True
    else:
        return False


def fix_location(mount_point, journal_node, osdid):
    """ Move the journal to the new partition """
    # Fix symlink
    path = mount_point + "/journal"  # 'journal' symlink path used by ceph-osd
    new_target = DISK_BY_PARTUUID + get_partition_uuid(journal_node)
    params = {"path": path, "target": new_target}
    try:
        if os.path.lexists(path):
            os.unlink(path)  # delete the old symlink
        os.symlink(new_target, path)
        print "Symlink created: %(path)s -> %(target)s" % params
    except:
        print "Failed to create symlink: %(path)s -> %(target)s" % params
        exit(1)

    # Clean the journal partition
    # even if erasing the partition table, is another journal was present here
    # it's going to be reused. Journals are always bigger than 100MB
    command(['dd', 'if=/dev/zero', 'of=%s' % journal_node,
             'bs=1M', 'count=100'])

    # Format the journal
    cmd = ['/usr/bin/ceph-osd', '-i', str(osdid),
           '--pid-file', '/var/run/ceph/osd.%s.pid' % osdid,
           '-c', '/etc/ceph/ceph.conf',
           '--cluster', 'ceph',
           '--mkjournal']
    out, err, ret = command(cmd)
    params = {"journal_node": journal_node,
              "osdid": osdid,
              "ret": ret,
              "reason": err}
    if not ret:
        print ("Prepared new journal partition: %(journal_node)s "
               "for osd id: %(osdid)s") % params
    else:
        print ("Error initializing journal node: "
               "%(journal_node)s for osd id: %(osdid)s "
               "ceph-osd return code: %(ret)s reason: %(reason)s" % params)


########
# Main #
########

def main(argv):
    # parse and validate arguments
    err = False
    partitions = None
    location = None
    if len(argv) != 2:
        err = True
    elif argv[0] == "partitions":
        valid_keys = ['disk_node', 'journals']
        partitions = get_input(argv[1], valid_keys)
        if not partitions:
            err = True
        elif not isinstance(partitions['journals'], list):
            err = True
    elif argv[0] == "location":
        valid_keys = ['data_node', 'journal_node', 'osdid']
        location = get_input(argv[1], valid_keys)
        if not location:
            err = True
        elif not isinstance(location['osdid'], int):
            err = True
    else:
        err = True
    if err:
        print "Command intended for internal use only"
        exit(-1)

    if partitions:
        # Recreate partitions only if the existing ones don't match input
        if not is_partitioning_correct(partitions['disk_node'],
                                       partitions['journals']):
            create_partitions(partitions['disk_node'], partitions['journals'])
        else:
            print ("Partition table for %s is correct, "
                   "no need to repartition" % partitions['disk_node'])
    elif location:
        # we need to have the data partition mounted & we can let it mounted
        mount_point = mount_data_partition(location['data_node'],
                                           location['osdid'])
        # Update journal location only if link point to another partition
        if not is_location_correct(mount_point,
                                   location['journal_node'],
                                   location['osdid']):
            print ("Fixing journal location for "
                   "OSD id: %(id)s" % {"node": location['data_node'],
                                       "id": location['osdid']})
            fix_location(mount_point,
                         location['journal_node'],
                         location['osdid'])
        else:
            print ("Journal location for %s is correct,"
                   "no need to change it" % location['data_node'])

main(sys.argv[1:])