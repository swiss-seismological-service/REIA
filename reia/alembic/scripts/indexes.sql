CREATE INDEX idx_aggregationtag_name ON loss_aggregationtag (name);
CREATE INDEX idx_aggregationtag_type ON loss_aggregationtag (type);

CREATE INDEX idx_assoc_riskvalue_aggregationtag ON loss_assoc_riskvalue_aggregationtag (aggregationtag);
CREATE INDEX idx_assoc_riskvalue_aggregationtype ON loss_assoc_riskvalue_aggregationtag (aggregationtype);
CREATE INDEX idx_assoc_riskvalue_riskvalue ON loss_assoc_riskvalue_aggregationtag (riskvalue);

CREATE INDEX idx_calculation_status_type ON loss_calculation (status, _type);
CREATE INDEX idx_calculationbranch_calculation ON loss_calculationbranch (_calculation_oid);

CREATE INDEX idx_riskvalue_oid ON loss_riskvalue (_oid);
CREATE INDEX idx_riskvalue_type ON loss_riskvalue (_type);
CREATE INDEX idx_riskvalue_calculationbranch on loss_riskvalue (_calculationbranch_oid);
