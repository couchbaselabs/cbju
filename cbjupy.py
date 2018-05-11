from collections import defaultdict

import os
import sys

try:
    import nutshell
except ImportError:
    sys.path.append(os.path.expanduser('~/nutshell/nutshell')) # used during dev
    import nutshell

# more packages from nutshell
import results
import utils


def run():
    nutshell.parse_arguments([])

    log_locations = [f for f in os.listdir('.')
                     if not os.path.isfile(f) and f[0] != '.']

    r = process_log_locations(log_locations)

    for cluster_name, cluster_info in r['cluster_infos'].iteritems():
        print cluster_render(cluster_name, cluster_info['cluster_obj'])

    print nutshell.format_results(r['node_results'], r['cluster_results'], 'text')

    return r


def process_log_locations(log_locations = None):
    log_location_results = map(nutshell.parse_log_location, log_locations)

    node_results = dict()
    node_stats = dict()
    node_infos = dict()

    for r in log_location_results:
        if r:
            if 'ns_stats' in r:
                node_name = r['ns_stats'].get_node_name()
                node_results[node_name] = nutshell.get_node_results(r)
                node_stats[node_name] = nutshell.get_node_stats(r)
                node_infos[node_name] = dict()
            elif 'cblog' in r and hasattr(r['cblog'], 'hostname'):
                node_name = r['cblog'].hostname
                node_results[node_name] = nutshell.get_node_results(r)
                node_infos[node_name] = dict()

    node_names = node_infos.keys()
    node_names.sort()

    clusters = nutshell.aggregate_cluster_info(log_location_results, node_results, node_stats)

    cluster_results = dict()
    cluster_infos = dict()

    for c in clusters:
        cluster_name = c.summarize_name(c.nodes())
        cluster_results[cluster_name] = nutshell.get_cluster_results(c)
        cluster_infos[cluster_name] = {'cluster_name': cluster_name, 'cluster_obj': c}

        for n in c.nodes():
            node_infos[n]['cluster_name'] = cluster_name

    cluster_names = cluster_infos.keys()
    cluster_names.sort()

    return {'log_locations': log_locations,
            'log_location_results': log_location_results,
            'node_results': node_results,
            'node_stats': node_stats,
            'node_infos': node_infos,
            'node_names': node_names,
            'cluster_results': cluster_results,
            'cluster_infos': cluster_infos,
            'cluster_names': cluster_names}


def cluster_render(name, cluster):
    """Summarize the cluster to a table"""

    # Couchbase Server 4.0 introduced services (kv, index, n1ql)
    # check for any CB 4x servers
    if cluster.check_for_ver4_nodes():
        analyser_result = results.AnalyserResult('Cluster (' + name + ')',
                                                 sort_table=True)
        analyser_result.set_padding(2)

        headings = [results.TableHeading('Service'),
                    results.TableHeading('Total'),
                    results.TableHeading('Nodes')]

        analyser_result.add_headings(headings)

        services = defaultdict(list)

        for node in cluster.nodes():
            for service in utils.get_services_of_node(cluster.config, node):
                services[service].append(node)

        for service in services:
            if len(services[service]) == 1:
                analyser_result.add_row([service, 1,
                                         services[service][0].replace('ns_1@', '', 1)])
            else:
                analyser_result.add_row([service, len(services[service]),
                                         cluster.summarize_name(services[service])])

        return analyser_result.render()
