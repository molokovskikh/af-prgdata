delete from farm.BuyingMatrix
where priceId = 4957;

insert into farm.BuyingMatrix(PriceId, Code, ProductId, ProducerId)
select c0.PriceCode, c0.Code, c0.ProductId, c0.CodeFirmCr
from farm.Core0 c0
where pricecode = 4957
group by c0.ProductId, c0.CodeFirmCr;