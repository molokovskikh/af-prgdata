update
  farm.Regions
set
  TechContact = '<tr> <td class="contactText">тел.:<strong>260-60-00</strong></td> </tr>',
  TechOperatingMode = '<tr> <td class="contactText">будни: с 7.00 до 19.00</td> </tr>'
where
  RegionCode = 1;