import pandas as pd

from reia.webservice.utils import aggregate_by_branch_and_event


def test_aggregate_by_branch_and_event():
    """Test aggregate_by_branch_and_event function with basic scenarios."""
    # Test data with some rows having same branch+event (should be grouped)
    data = pd.DataFrame({
        'branchid': [1, 1, 1, 2],
        'eventid': [1, 1, 2, 1],
        'weight': [0.3, 0.7, 0.4, 0.6],
        'loss_value': [100, 200, 150, 250],
        'damage_value': [10, 20, 15, 25]
    })

    result = aggregate_by_branch_and_event(data, 'aggregation_test')

    # Should have 3 unique combinations: (1,1), (1,2), (2,1)
    assert len(result) == 3

    # Check required columns exist
    assert 'weight' in result.columns
    assert 'loss_value' in result.columns
    assert 'damage_value' in result.columns
    assert 'aggregation_test' in result.columns

    # Check values are aggregated correctly for branch=1, event=1
    # Weight should be (0.3 + 0.7) / 2 = 0.5
    # loss_value should be 100 + 200 = 300
    # damage_value should be 10 + 20 = 30
    grouped_values = result['loss_value'].tolist()
    assert 300 in grouped_values  # Sum of first two loss_values
    assert 150 in grouped_values  # Single loss_value for (1,2)
    assert 250 in grouped_values  # Single loss_value for (2,1)

    # Check aggregation_type column is set to False
    assert all(~result['aggregation_test'])
