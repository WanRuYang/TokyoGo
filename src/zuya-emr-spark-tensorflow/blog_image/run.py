from __future__ import print_function
import numpy as np
import tensorflow as tf
import os
from tensorflow.python.platform import gfile
import os.path
import re
from pyspark import SparkContext


def read_img_metadata_batches(batch_size):
    """
    Read input image metadata (id, URL) and output as batches
    (Ex. batch size = 3)
    :return:
        [
            [(img_id0, img_url0), (img_id1, img_url1), (img_id2, img_url2)],
            [(img_id3, img_url3), (img_id4, img_url4), (img_id5, img_url5)]
        ]
    """
    with open("imagenet.tsv", 'r') as f:
        lines = [l.split() for l in f]
        input_data = [tuple(elts) for elts in lines if len(elts) == 2]
        return [input_data[i:i+batch_size] for i in range(0,len(input_data), batch_size)]


def load_lookup(uid_lookup_path, label_lookup_path):
    """Loads a human readable English name for each softmax node.

    Args:
      label_lookup_path: string UID to integer node ID.
      uid_lookup_path: string UID to human-readable string.

    Returns:
      dict from integer node ID to human-readable string.
    """
    if not gfile.Exists(uid_lookup_path):
        tf.logging.fatal('File does not exist %s', uid_lookup_path)
    if not gfile.Exists(label_lookup_path):
        tf.logging.fatal('File does not exist %s', label_lookup_path)

    # Loads mapping from string UID to human-readable string
    proto_as_ascii_lines = gfile.GFile(uid_lookup_path).readlines()
    uid_to_human = {}
    p = re.compile(r'[n\d]*[ \S,]*')
    for line in proto_as_ascii_lines:
        parsed_items = p.findall(line)
        uid = parsed_items[0]
        human_string = parsed_items[2]
        uid_to_human[uid] = human_string

    # Loads mapping from string UID to integer node ID.
    node_id_to_uid = {}
    proto_as_ascii = gfile.GFile(label_lookup_path).readlines()
    for line in proto_as_ascii:
        if line.startswith('  target_class:'):
            target_class = int(line.split(': ')[1])
        if line.startswith('  target_class_string:'):
            target_class_string = line.split(': ')[1]
            node_id_to_uid[target_class] = target_class_string[1:-2]

    # Loads the final mapping of integer node ID to human-readable string
    node_id_to_name = {}
    for key, val in node_id_to_uid.items():
        if val not in uid_to_human:
            tf.logging.fatal('Failed to locate: %s', val)
        name = uid_to_human[val]
        node_id_to_name[key] = name

    return node_id_to_name


def run_image(sess, img_id, img_url, node_lookup):
    from six.moves import urllib
    from urllib2 import HTTPError

    num_top_predictions = 5

    try:
        image_data = urllib.request.urlopen(img_url, timeout=1.0).read()
    except HTTPError:
        return (img_id, img_url, None)
    except:
        return (img_id, img_url, None)

    softmax_tensor = sess.graph.get_tensor_by_name('softmax:0')
    predictions = sess.run(softmax_tensor,
                           {'DecodeJpeg/contents:0': image_data})
    predictions = np.squeeze(predictions)

    top_k = predictions.argsort()[-num_top_predictions:][::-1]
    scores = []
    for node_id in top_k:
        if node_id not in node_lookup:
            human_string = ''
        else:
            human_string = node_lookup[node_id]
        score = predictions[node_id]
        scores.append((human_string, score))
    return (img_id, img_url, scores)


def apply_batch(batch):
    with tf.Graph().as_default() as g:
        graph_def = tf.GraphDef()
        graph_def.ParseFromString(model_data_bc.value)
        tf.import_graph_def(graph_def, name='')
        with tf.Session() as sess:
            labelled = [run_image(sess, img_id, img_url, node_lookup_bc.value) for (img_id, img_url) in batch]
            return [tup for tup in labelled if tup[2] is not None]


def print_results(labeled_imgs):
    print('Results:')
    for i, labeled_img in enumerate(labeled_imgs):
        (img_id, url, labels) = labeled_img
        print('Image #{}: id: {}, url: {}'.format(i + 1, img_id, url))
        for label in labels:
            (label_str, label_score) = label
            print('  ({:.3f}) {}'.format(label_score, label_str))


if __name__ == '__main__':

    model_dir = 'imagenet'

    sc = SparkContext('yarn', 'blog_image')

    # Construct and share node ID (image class) to human-readable label table
    label_lookup_path = os.path.join(model_dir, 'imagenet_2012_challenge_label_map_proto.pbtxt')
    uid_lookup_path = os.path.join(model_dir, 'imagenet_synset_to_human_label_map.txt')
    node_lookup = load_lookup(uid_lookup_path, label_lookup_path)
    node_lookup_bc = sc.broadcast(node_lookup)

    # Share model
    model_path = os.path.join(model_dir, 'classify_image_graph_def.pb')
    with gfile.FastGFile(model_path, 'rb') as f:
        model_data = f.read()

    model_data_bc = sc.broadcast(model_data)

    # Distribute image metadata
    batched_data = read_img_metadata_batches(batch_size=3)
    img_meta_rdd = sc.parallelize(batched_data)

    # Compute
    labeled_imgs_rdd = img_meta_rdd.flatMap(apply_batch)
    local_labeled_imgs = labeled_imgs_rdd.collect()
    print_results(local_labeled_imgs)
