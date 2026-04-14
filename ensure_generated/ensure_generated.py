import sys

import boto3

s3client = boto3.client('s3')
paginator = s3client.get_paginator('list_objects_v2')

# Since June 2017, Nutch intermediate crawl data isn't kept
# on the public data set bucket
bucket = 'commoncrawl-nutch'

# Get all segments
# s3://commoncrawl-nutch/segments.20150929/
# target = "segments.20150929"
target = str(sys.argv[1])
if not target.endswith('/'):
  target += '/'
expected_fetch_lists = 400
if len(sys.argv) >= 3:
  expected_fetch_lists = int(sys.argv[2])
expected_segments = 100
if len(sys.argv) >= 4:
  expected_segments = int(sys.argv[3])

segments = []
for page in paginator.paginate(Bucket=bucket, Prefix=target, Delimiter='/'):
  for prfx in page.get('CommonPrefixes') or []:
    segments.append(prfx['Prefix'])

if not segments:
  sys.stderr.write('No object found on S3. Make sure the selected segment exists.\n')
  sys.exit(1)
print('Total of {} segments'.format(len(segments)))
print('Expected number of fetch lists per segment: {}'.format(expected_fetch_lists))

good, bad = 0, 0

d = {}
seg_sizes = []
dead_segs = set()
granular_error_messages = []
for i, segment in enumerate(segments):
  sys.stderr.write('\rProcessing segment {} of {} ({})'.format(i, len(segments), segment))
  fetchlists = []
  for page in paginator.paginate(Bucket=bucket, Prefix=segment + 'crawl_generate/', Delimiter='/'):
    fetchlists.extend(page.get('Contents') or [])

  if not fetchlists:
    bad += 1
    sys.stderr.write('\n')
    granular_error_messages.append('{} has no fetchlists at all'.format(segment))
    dead_segs.add(segment)
  elif len(fetchlists) != expected_fetch_lists:
    bad += 1
    sys.stderr.write('\n')
    granular_error_messages.append('{} has {} fetchlists'.format(segment, len(fetchlists)))
    dead_segs.add(segment)
  else:
    seg_size = 0
    has_bad_entries = False
    for fetch_partition in fetchlists:
      fetch_list_size = fetch_partition['Size']
      name = fetch_partition['Key'].split("/")[-1]
      if fetch_list_size > 0:
        good += 1
        seg_size += fetch_list_size
      else:
        bad += 1
        has_bad_entries = True
        dead_segs.add(segment)
        sys.stderr.write('\n')
        granular_error_messages.append('{} has fetchlist {} with size of 0'.format(segment, name))

    if not has_bad_entries:
      if seg_size not in d:
        d[seg_size] = []
      d[seg_size].append(segment)
      seg_sizes.append((segment, seg_size))
sys.stderr.write('\n')

seg_sizes = sorted(seg_sizes, key=lambda x: x[1])

print('Total good fetchlists: {}'.format(good))
print('Total bad fetchlists: {}'.format(bad))
print('Total clean segments: {}'.format(len(segments) - len(dead_segs)))
print('Total dead segments: {}'.format(len(dead_segs)))

if len(seg_sizes) != 0:
  print('Average size: {}'.format(sum(x[1] for x in seg_sizes) / len(seg_sizes)))
print('Unique total size for segments: {}'.format(sum(set(x[1] for x in seg_sizes))))

# rstrip the segment ends as sometimes we do silly tricks to get the segment name
# i.e. rev | cut -d '/' -f 1 | rev

good_segs = set()
with open('/tmp/good_segs', 'w') as f:
  for size in d:
    good_seg = d[size][-1]
    good_segs.add(good_seg)
    f.write('s3a://{}/{}\n'.format(bucket, good_seg.rstrip('/')))

all_segs = set(x[0] for x in seg_sizes)
bad_segs = all_segs - good_segs | dead_segs
with open('/tmp/bad_segs', 'w') as f:
  for seg in bad_segs:
    s3seg = 's3://{}/{}\n'.format(bucket, seg.rstrip('/'))
    f.write(s3seg)

if len(bad_segs) != 0 or len(good_segs) != expected_segments:
  print('Issues: ')
  for msg in granular_error_messages:
    print(f' - {msg}')

  # exit with error to stop crawl workflow, manual interaction required
  print('Need to fix segments:')
  print('- delete bad segments listed in /tmp/bad_segs')
  print('- verify segments listed in /tmp/bad_segs')
  sys.exit(1)
