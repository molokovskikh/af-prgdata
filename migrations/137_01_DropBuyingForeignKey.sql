truncate farm.BuyingMatrix;

alter table farm.BuyingMatrix
  drop FOREIGN KEY FK_BuyingMatrix_CatalogId,
  drop FOREIGN KEY FK_BuyingMatrix_PriceId,
  drop FOREIGN KEY FK_BuyingMatrix_ProducerId;

alter table farm.BuyingMatrix
  drop key FK_BuyingMatrix_Comb;

alter table farm.BuyingMatrix
  drop key FK_BuyingMatrix_CatalogId,
  drop key FK_BuyingMatrix_ProducerId;
