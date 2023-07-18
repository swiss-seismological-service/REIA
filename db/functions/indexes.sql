CREATE INDEX idx_loss_aggregationtag_name ON loss_aggregationtag (name);
CREATE INDEX idx_loss_aggregationtag_type ON loss_aggregationtag (type);

CREATE INDEX idx_loss_assoc_riskvalue_aggregationtag ON loss_assoc_riskvalue_aggregationtag (aggregationtag);
CREATE INDEX idx_loss_assoc_riskvalue_aggregationtype ON loss_assoc_riskvalue_aggregationtag (aggregationtype);
CREATE INDEX idx_loss_assoc_riskvalue_riskvalue ON loss_assoc_riskvalue_aggregationtag (riskvalue);

CREATE INDEX idx_loss_calculation_status_type ON loss_calculation (status, _type);

CREATE INDEX idx_loss_riskvalue_oid ON loss_riskvalue (_oid);
CREATE INDEX idx_loss_riskvalue_type ON loss_riskvalue (_type);
