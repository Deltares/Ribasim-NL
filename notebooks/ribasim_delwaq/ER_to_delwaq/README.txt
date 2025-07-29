### Databronnen ###

Bestandlocaties in python scripts dienen te worden aangepast

Conversiescript ER_data_conversion_delwaq.py is bewerking van p:/krw-verkenner/01_landsdekkende_schematisatie/LKM25 schematisatie/OverigeEmissies/KRW_Tussenevaluatie_2024/Convert_ER_Emissions_To_KRW_input_tusseneval.py"

- Bewerkt om nieuw koppelscript te gebruiken, wordt aangehaald via functie uit python script ER_GAF_fractions_func.py

- GAF polygonen afkomstig van P:/11210327-lwkm2/01_data/Emissieregistratie/gaf_90.shp

- Script verder onveranderd, pakt dataframe 'Diffuse_emissions_OE' om delwaq input te genereren



### Stappenplan ER koppeling ###

1. download data
	ER export
	GAF polygonen
	Ribasim schematisatie
2. Optioneel: pyQGIS script draaien (via QGIS) ER_GAF_fractions_via_QGIS.py
3. ER conversiescript draaien ER_data_conversion_delwaq.py
	Dit levert B6.inc op
4. ER_setup_delwaq.py draaien
	Dit levert bndlist.inc op voor de loads in B6
5. B6 en bndlist in delwaq map plaatsen
6. handmatig het .inp bestand aanpassen:

	B1: 	'N' en 'P' als substances toevoegen
			totaal aantal stoffen +2

	B6: 	verwijder 0; number of loads
			toevoegen:	INCLUDE delwaq.bndlist.inc
						INCLUDE B6_loads_on_basins.inc

	B8:		alles weghalen
			toevoegen:	INITIALS{alle stoffen zonder '' met enkel spaties ertussen}
						{de IC waarden met spaties ertussen}

7. delwaq runnen via cmd of python
8. ER_run_parse_inspect.py draaien voor postprocessing/validatie:
	check substances en run eventueel:
		substances.add 'N'
		substances.add 'P'
	code cell vanaf parse draaien
