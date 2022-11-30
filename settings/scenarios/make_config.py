import json

import pandas as pd

EXPOSUREFILE = {
    'MIM': {
        'RB': 1,
        'RF': 1
    },
    'SAM': {
        'RB': 3,
        'RF': 3
    }
}


def make_config():
    df = pd.read_csv('config.csv')
    config = []

    for scenario, group in df.groupby('OriginID'):
        config_scenario = {
            'scenario_name': f"Szenario {scenario.split('/')[-1]}",
            'folder': '',
            'originid': scenario,
            'damage': [],
            'loss': []
        }
        for calc_type, branch in group.groupby('calc_type'):
            for _, row in branch.iterrows():
                config_branch = {
                    'store': row['file'],
                    'weight': row['weight'],
                    'exposure': EXPOSUREFILE[row['model']][row['exposure']]
                }
                if calc_type == 'damage':
                    config_scenario['damage'].append(config_branch)
                else:
                    config_scenario['loss'].append(config_branch)
        config.append(config_scenario)

    with open('config.json', 'w') as f:
        f.write(json.dumps(config))


if __name__ == '__main__':
    make_config()
