from datamodel import LossModel, LossConfig
import json


def test_loss_config():
    with open('tests/data/risk.json') as f:
        data = json.load(f)
    loss_model = LossModel(**data)
    loss_config = LossConfig(lossCategory='structural',
                             aggregateBy='site', lossModel=loss_model)
    data['lossCategory'] = 'structural'
    data['aggregateBy'] = 'site'

    assert loss_config.to_dict() == data
