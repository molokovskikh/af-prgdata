DROP FUNCTION usersettings.MakeCostNoLess;
CREATE DEFINER=`RootDBMS`@`127.0.0.1` FUNCTION usersettings.`MakeCostNoLess`(selfCost decimal(11, 2), opponentCost decimal(11, 2), ruleId INT unsigned) RETURNS decimal(11,2)
    NO SQL
BEGIN

declare distance decimal(11, 2);
declare minAmount decimal(11, 2);
declare minPercent decimal(11, 2);
declare maxPercent decimal(11, 2);
declare minDelta decimal(11, 2);
declare maxDelta decimal(11, 2);

if opponentCost is null then
  return selfCost;
end if;

SELECT u.MinAmount, u.MinPercent, u.MaxPercent, u.MinDelta, u.MaxDelta
into minAmount, minPercent, maxPercent, minDelta, maxDelta
FROM usersettings.costoptimizationrules u where Id=ruleId;

if minAmount is null or minPercent is null or maxPercent is null
or minDelta is null or maxDelta is null then
 return selfCost;
end if;

if selfCost <= minAmount then
	return selfCost;
end if;

set distance = (opponentCost - selfCost) * 100 / opponentCost;

if distance <= minPercent then

  return selfCost;

end if;

if distance < maxPercent and distance > minPercent then

  return opponentCost - opponentCost * (floor(minDelta * 10 + rand() * (maxDelta - minDelta) * 10) / 1000);

end if;
return selfCost;

END;
