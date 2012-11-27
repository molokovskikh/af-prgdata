DROP FUNCTION usersettings.MakeCostNoLess;
CREATE DEFINER=`RootDBMS`@`127.0.0.1` FUNCTION usersettings.`MakeCostNoLess`(selfCost decimal(11, 2), opponentCost decimal(11, 2)) RETURNS decimal(11,2)
    NO SQL
BEGIN

declare distance decimal(11, 2);

if opponentCost is null then
  return selfCost;
end if;

if selfCost <= 30 then
	return selfCost;
end if;

set distance = ((selfCost - opponentCost) * 100) / opponentCost;

if distance > -0.8 and distance < 0 then

  return selfCost;

end if;

if distance > -23 and distance < -0.8 then

  return opponentCost - opponentCost * (floor(2 + rand() * 5) / 1000);

end if;

return selfCost;

END;
