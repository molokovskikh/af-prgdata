alter table Farm.Rejects
add column UpdateTime timestamp not null default current_timestamp on update current_timestamp;
