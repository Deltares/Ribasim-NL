### DIMR_PATH environment variable ###

Om de conversie van ER naar Delwaq uit te voeren, zorg ervoor dat de
environment variable DIMR_PATH is ingesteld op het pad naar de DIMR executable.
Dit is vereist om de conversiescripts te kunnen draaien.

Voorbeeld:

DIMR_PATH=c:\Program Files\Deltares\Delft3D FM Suite 2025.02 HMWQ\plugins\DeltaShell.Dimr\kernels\x64\bin\run_dimr.bat



### Databronnen ###

Bestandlocaties in python scripts dienen te worden aangepast

Conversiescript ER_data_conversion_delwaq.py is bewerking van p:/krw-verkenner/01_landsdekkende_schematisatie/LKM25 schematisatie/OverigeEmissies/KRW_Tussenevaluatie_2024/Convert_ER_Emissions_To_KRW_input_tusseneval.py"

- Bewerkt om nieuw koppelscript te gebruiken, wordt aangehaald via functie uit python script ER_GAF_fractions_func.py

- GAF polygonen afkomstig van P:/11210327-lwkm2/01_data/Emissieregistratie/gaf_90.shp

- Script verder onveranderd, pakt dataframe 'Diffuse_emissions_OE' om delwaq input te genereren



### Stappenplan ER koppeling ###

1. download Ribasim model via notebooks/rwzi/add_rwzi_model.py

	zorg ervoor dat de environment variable "RIBASIM_NL_DATA_DIR" wordt gebruikt als locatie

2. draai Ribasim model voor gewenste periode (mag kort zijn voor tests)

	periode aan te passen in .toml bestand van het model

3. draai ER_setup_delwaq.py

	dit levert de meeste input bestanden voor delwaq via generate.py

	genereert los delwaq_bndlist.inc

4. draai ER_data_conversion_delwaq.py

	dit levert B6_loads.inc op

6. handmatig het delwaq.inp aanpassen:

	B1: 	'N' en 'P' als substances toevoegen
			totaal aantal stoffen +2

	B6: 	verwijder 0; number of loads
			toevoegen:	INCLUDE delwaq_bndlist.inc
						INCLUDE B6_loads.inc

	B8:		alles weghalen
			toevoegen:	INITIALS {alle stoffen zonder '' met enkel spaties ertussen}
						DEFAULTS {de IC waarden met spaties ertussen}
			voorbeeld: 	INITIALS Continuity Drainage FlowBoundary Initial LevelBoundary Precipitation Terminal UserDemand N P
						DEFAULTS 1.0 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 0.0

7. delwaq runnen via cmd of python

8. ER_run_parse_inspect.py draaien voor postprocessing/validatie:
	check substances en run eventueel:
		substances.add 'N'
		substances.add 'P'
