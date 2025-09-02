# Analýza súborov Excel

Tento dokument sumarizuje zistenia z analýzy dvoch súborov Excel: `ronec_dochadzka.xlsx` a `ronec_vykaz.xlsx`.

## Zdrojový súbor (`ronec_dochadzka.xlsx`)

- **Počet riadkov a stĺpcov:** 50 riadkov, 8 stĺpcov.
- **Obsah:** Súbor obsahuje slovenské texty, informácie o firme "PERRY SOFT a.s." a zamestnancovi "Simon Ronec".
- **Obdobie:** Zdá sa, že ide o mesiac "Júl 2025".
- **Kľúčové stĺpce:** Obsahuje stĺpce pre dátumy, časy príchodu ("Príchod") a skutočne odpracovaný čas ("Skutočný odpracovaný čas").

## Cieľový súbor (`ronec_vykaz.xlsx`)

- **Počet riadkov a stĺpcov:** 72 riadkov, 14 stĺpcov.
- **Obsah:** Súbor vyzerá ako šablóna formulára s prevažne prázdnymi bunkami.
- **Štruktúra:** Obsahuje stĺpec s názvom "Príloha č. 3" a jeho štruktúra naznačuje, že ide o formálnu šablónu pracovného výkazu.

## Otvorené otázky pre mapovanie dát

Pre správne mapovanie dát je potrebné zodpovedať nasledujúce otázky:

1.  **Formát dátumu:** Ako sú reprezentované dátumy v zdrojovom súbore?
2.  **Identifikácia zamestnanca:** Ako sa majú mapovať mená zamestnancov?
3.  **Štruktúra pracovných hodín:** Ktoré stĺpce v zdrojovom súbore obsahujú časy príchodu, odchodu, prestávky a celkový denný odpracovaný čas?
4.  **Rozloženie cieľovej šablóny:** Ktoré riadky/stĺpce v cieľovom súbore majú obsahovať informácie o zamestnancovi, denné záznamy a súhrnné súčty?
5.  **Pravidlá transformácie dát:** Existujú nejaké špecifické požiadavky na výpočty alebo formátovanie pri prenose dát (napr. konverzie formátu času, pravidlá zaokrúhľovania, výpočty nadčasov)?