import sys
###
import boto
conn = boto.connect_s3(anon=True)

# Since April 2016, the public dataset bucket is s3://commoncrawl 
# (migrated from s3://aws-publicdatasets/common-crawl)
pds = conn.get_bucket('commoncrawl')

# Get all segments
# s3://commoncrawl/nutch/segments.20150929/
# target = "nutch/segments.20150929"
target = str(sys.argv[1])
expected_fetch_lists = 400
if len(sys.argv) >= 3:
  expected_fetch_lists = int(sys.argv[2])
segments = list(pds.list(target, delimiter='/'))
print 'Total of {} segments'.format(len(segments))
print('Expected number of fetch lists per segment: {}'.format(expected_fetch_lists))

good, bad = 0, 0

d = {}
seg_sizes = []
dead_segs = set()
for i, segment in enumerate(segments):
  sys.stderr.write('\rProcessing segment {} of {}'.format(i, len(segments)))
  fetchlists = list(pds.list(segment.name + 'crawl_generate/'))
  if len(fetchlists) != expected_fetch_lists:
    bad += 1
    sys.stderr.write('\n')
    sys.stderr.write('{} has {} fetchlists\n'.format(segment.name, len(fetchlists)))
    dead_segs.add(segment.name)
  else:
    good += 1
    seg_size = sum(x.size for x in fetchlists)
    if seg_size not in d:
      d[seg_size] = []
    d[seg_size].append(segment.name)
    seg_sizes.append((segment, seg_size))
sys.stderr.write('\n')

seg_sizes = sorted(seg_sizes, key=lambda x: x[1])

print 'Total good segments: {}'.format(good)
print 'Total bad segments: {}'.format(bad)
print 'Total dead segments: {}'.format(len(dead_segs))
print 'Average size: {}'.format(sum(x[1] for x in seg_sizes) / len(seg_sizes))
print 'Unique total sizes for segments: {}'.format(len(set(x[1] for x in seg_sizes)))

# rstrip the segment ends as sometimes we do silly tricks to get the segment name
# i.e. rev | cut -d '/' -f 1 | rev

good_segs = set()
with open('/tmp/good_segs', 'w') as f:
  for size in d:
    good_seg = d[size][-1]
    good_segs.add(good_seg)
    f.write('s3a://commoncrawl/{}\n'.format(good_seg.rstrip('/')))

all_segs = set(x[0].name for x in seg_sizes)
bad_segs = all_segs - good_segs | dead_segs
with open('/tmp/bad_segs', 'w') as f:
  for seg in bad_segs:
    f.write('s3://commoncrawl/{}\n'.format(seg.rstrip('/')))
