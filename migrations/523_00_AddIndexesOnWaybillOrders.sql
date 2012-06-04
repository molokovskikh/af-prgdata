alter table documents.waybillorders
  add key `IDX_waybillorders_DocumentLineId` (`DocumentLineId`),
  add key `IDX_waybillorders_OrderLineId` (`OrderLineId`);