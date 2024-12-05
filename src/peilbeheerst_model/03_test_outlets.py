from outlets import case1, case2

# # Case 1

# ### Example 1: boundary and basin levels on target


case1_example1 = case1("case1_example1")
case1_example1.create_model()


# ### Example 2: boundary levels below target


case1_example2 = case1("case1_example2")
case1_example2.create_model()


# ### Example 3: boundary levels on target, initial state below target


case1_example3 = case1("case1_example3")
case1_example3.create_model()


# ### Example 4: boundary levels on target, initial state above target


case1_example1 = case1("case1_example4")
case1_example1.create_model()


# # Case 2

# ### Example 1: boundary and basin levels on target


# first, load in the simple model of case 1. Copy it.
case2_example1 = case1("case2_example1")
case2_base_model = case2_example1.create_model(copy=True)

# then, change it to a case 2 category
case2_example1 = case2("case2_example1", model=case2_base_model)
case2_example1.create_model()


# ### Example 2: boundary and basins below target, third basin above
# The third basins should fill up the others, or else be pumped to target level by the second pump.


# first, load in the simple model of case 1. Copy it.
case2_example1 = case1("case2_example2")
case2_base_model = case2_example1.create_model(copy=True)

case2_example2 = case2("case2_example2", model=case2_base_model)
case2_example2.create_model()


# ### Example 3: boundary and basins below target, third basin above, pump rate of third peilgebied set to 0.
# Quite similair to case 2 example 2, exvept that the pump rate of first pump set to a low rate, so a rising water level is expected in the other basins. The third basins should fill up the others.


# first, load in the simple model of case 1. Copy it.
case2_example1 = case1("case2_example3")
case2_base_model = case2_example1.create_model(copy=True)

# implement the second model
case2_example3 = case2("case2_example3", model=case2_base_model)
case2_example3.create_model()


# ### Example 4: low target level in third basin, results in incorrect flow direction
#
# The water level in each basin, including the boundaries, are too low. No water should be flowing. However, only the initial level is higher than the  target level of the third basin. Water should only flow from the third basin to the last level boundary.


# first, load in the simple model of case 1. Copy it.
case2_example4 = case1("case2_example4")
case2_base_model = case2_example4.create_model(copy=True)

# implement the second model
case2_example4 = case2("case2_example4", model=case2_base_model)
case2_example4.create_model()

# Volgende case zou kunnen zijn dat er tussen twee peilgebieden (met wel of niet andere target levels) er outlets zijn, die van de een naar de ander gaan, en andersom. Maar in principe verwacht ik daar geen gekke situaties: de outlet laat alleen water stromen als dit onder vrij verval kan, en pompt geen water omhoog. Het enige wat wel gek zou kunnen worden, is als beide peilen rond hetzelfde niveau komen. Dan zou het water de ene tijdstap van links naar rechts kunnen stromen, en de andere momenten de andere kant op. Ik kan me voorstellen dat dit tot instabiliteiten leidt. Weet alleen niet zeker of dit gaat optreden bij simpele voorbeelden als hier.Wat wel interessant zou zijn is het toch wel toevoegen van ContinuousControls. Eerst leek dit niet een logische stap, omdat ik wilde dat de outlet zou luisteren naar boven- en benedenstroomse peil. Maar nu doet dat het eigenlijk alleen naar bovenstrooms.

# Punt van reflectie: is dat uberhaupt wel de goede aanpak? Hoe weet de basin dan dat het water moet doorlaten? Je kan dit doen door de crest level een stukje lager te zetten dan streefpeil, maar dat houdt dan wel in dat er ALTIJD water stroomt. Qua doorspoeling is dat opzich nog zo gek niet, maar het is niet de meest chique manier. Ook heb je hier dan weinig controle op, wat je misschien wel wil hebben.
# Conclusie(?): de discrete controls moeten OOK gaan luisteren naar benedenstroomse peil. Dit toch wel doen aan de hand van de vier verschillende opties, afhankelijk wat de streefpeil van peilgebied 1 en peilgebied 2 is.

# Om instabiliteiten tegen te gaan is het wellicht goed om de grenzen niet precies op streefpeil te zetten, maar juist met een bepaalde afwijking. De afwijking om water door te mogen voeren van peilgebied 1 naar peilgebied2 moet kunnen zolang het waterniveau van peilgebied 1 niet 5 cm onder streefpeil zakt. De inlaat vanaf de boezem naar peilgebied 1 moet in principe sturen op exact streefpeil. 1) Hoe verhoudt dit zich tot de min_crest_level en een enkele listen_to_node?

# De min_crest_level voert met 1 listen_node_id altijd water door. In het geval van hierboven zou water altijd van peilgebied 1 naar peilgebied 2 gaan, terwijl peilgebied 2 wellicht helemaal geen water nodig heeft terwijl de inlaat van de boezem naar peilgebied 1 wel water aan het inlaten is voor peilgebied 2.


# 2) Hoe verhoudt de min_crest_level zich met dubbele listen_to_nodes?

# Min_Crest_level lijkt me bijna overbodig worden.


# 3) Stel er komt alleen een min_crest_level op de genoemde 5 cm onder streefpeil van peilgebied 1, dat wordt als het ware een schaduw listen node. Dan hoeft de outlet alleen nog maar te luisteren naar de basin die benedenstrooms ligt?

# Dat is niet waar, want stel er is een minimum crest level gedefinieerd die 5 cm onder streefpeil ligt van peilgebied 1. Wat als de water stand 2 cm onder streefpeil ligt (dus wel nog boven crest level), en er is geen water nodig in peilgebied 2? --> dan blijft het stromen, terwijl dat niet moet.Conclusie: vorige conclusie is correct. Luisteren naar zowel boven- als benedestrooms.Stappenplan voor AGV:

# Loopen per DiscreteControl lijkt mij geen goed idee.


# # Thrashbin


example1_characteristics = {}


# solver
example1_characteristics["starttime"] = "2024-01-01 00:00:00"
example1_characteristics["endtime"] = "2024-01-03 00:00:00"
example1_characteristics["saveat"] = 60

# boezem settings (LevelBoundary)
example1_characteristics["boezem1_level"] = 3
example1_characteristics["boezem2_level"] = 3

# peilgebied settings (Basins)
example1_characteristics["basin1_profile_area"] = [0.01, 10000.0]
example1_characteristics["basin1_profile_level"] = [1.0, 5.0]
example1_characteristics["basin1_initial_level"] = [3]
example1_characteristics["basin1_target_level"] = [2]


example1_characteristics["basin2_profile_area"] = [0.01, 10000.0]
example1_characteristics["basin2_profile_level"] = [0.0, 5.0]
example1_characteristics["basin2_initial_level"] = [2]
example1_characteristics["basin2_target_level"] = [1]

example1_characteristics["evaporation"] = 5  # mm/day, will be converted later to m/s
example1_characteristics["precipitation"] = 5  # mm/day, will be converted later to m/s

# connection node settings (Outlets, Pumpts)
example1_characteristics["outlet1_flow_rate"] = 0.010
example1_characteristics["outlet1_min_crest_level"] = 2.90

example1_characteristics["outlet2_flow_rate"] = 0.010
example1_characteristics["outlet2_min_crest_level"] = 1.90

example1_characteristics["pump_flow_rate"] = 10 / 60  # [x] m3 / minute

# general settings
example1_characteristics["plot"] = True
example1_characteristics["crs"] = "EPSG:4326"
example1_characteristics["case"] = "case1"
example1_characteristics["example"] = "example1"
example1_characteristics["results_dir"] = r"../../../../Outlet_tests/"
example1_characteristics["show_progress"] = False
example1_characteristics["show_results"] = True

# solver settings
example1_characteristics["saveat"] = 60
