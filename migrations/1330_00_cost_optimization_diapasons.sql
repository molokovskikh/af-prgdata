create table UserSettings.CostOptimizationDiapasons (
        Id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
       Begin DECIMAL(19,5),
       End DECIMAL(19,5),
       Markup DECIMAL(19,5),
       RuleId INTEGER UNSIGNED,
       primary key (Id)
    );
alter table UserSettings.CostOptimizationDiapasons add index (RuleId), add constraint FK_UserSettings_CostOptimizationDiapasons_RuleId foreign key (RuleId) references UserSettings.CostOptimizationRules (Id) on delete cascade;
