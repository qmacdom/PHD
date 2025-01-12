import pickle
from dbm import error

import pandas as pd
import random
from datetime import datetime


# Funkcja do generowania grup wiekowych
def generate_age_group():
    age_groups = [
        (0, 10), (10, 17), (18, 27), (28, 40), (41, 65), (65, 102)
    ]
    probabilities = [0.1, 0.15, 0.3, 0.25, 0.15, 0.05]
    group = random.choices(age_groups, probabilities)[0]
    return f"{group[0]}-{group[1]}"


# Funkcja do generowania płci
def generate_gender():
    return random.choice(["M", "F"])


# Wczytaj dane z arkusza Excela
# file_path = "Baza_ruchu_granicznego_2023.xlsx"
# osoby_df = pd.read_excel(file_path, sheet_name="Osoby")
# cudzoziemcy_df = pd.read_excel(file_path, sheet_name="Cudzoziemcy",
#                                keep_default_na=False)
# with open('osoby_df.pkl', 'wb') as file:
#     pickle.dump(osoby_df,file)
# with open('cudzoziemcy.pkl', 'wb') as file:
#     pickle.dump(cudzoziemcy_df,file)

with open('osoby_df.pkl', 'rb') as file:
    osoby_df = pickle.load(file)
with open('cudzoziemcy.pkl', 'rb') as file:
    cudzoziemcy_df = pickle.load(file)

# Przygotowanie tabel
insert_queries = set()
inserted_crossing = []

# Usunięcie czasu z daty, jeśli występuje
osoby_df["Data"] = osoby_df["Data"].astype(str).str.split().str[0]

# W cudzoziemcy zmiana kierunku na poprawne wartości oraz zmiana nazwyh kolumny
cudzoziemcy_df.rename(columns={"Kierunek do/z RP": "Kierunek"}, inplace=True)
replacements = {'P': 'przyjazd', 'W': 'wyjazd'}
cudzoziemcy_df['Kierunek'] = cudzoziemcy_df['Kierunek'].map(replacements)

# Klucz glowny przejscia musi byc bardziej zlozony niz tylko nazwa dlatego wepchniemy rodzaj przejscia
osoby_df['PrzejscieKlucz'] = osoby_df['Przejście'] + osoby_df[
    'Rodzaj przejścia']
cudzoziemcy_df['PrzejscieKlucz'] = cudzoziemcy_df['Przejście'] + \
                                   cudzoziemcy_df['Rodzaj przejścia']

# Tabela Fakt_Przekroczenie_granicy
for _, osoba_row in osoby_df.iterrows():
    if osoba_row['Kto'] == 'C':
        # Znajdz odpowiedni rekord w cudzoziemcy_df na podstawie kluczowych pól
        matches = cudzoziemcy_df[
            (cudzoziemcy_df["Data"] == osoba_row["Data"]) &
            (cudzoziemcy_df["Kierunek"] == osoba_row["Kierunek"]) &
            (cudzoziemcy_df["PrzejscieKlucz"] == osoba_row["PrzejscieKlucz"])]
        if not matches.empty:

            non_zero_columns = []
            for col_name, value in osoba_row.items():
                if col_name not in ["Placówka SG", "Przejście",
                                    "Rodzaj przejścia",
                                    "Odcinek", "Oddział SG", "Data", "Kto",
                                    "Kierunek", "Razem"]:
                    try:
                        numeric_value = int(value)
                        if numeric_value > 0:
                            non_zero_columns.append((col_name, numeric_value))
                    except ValueError:
                        continue
            narodowosci = [(row['Obywatelstwo (kod)'], row['Razem']) for
                           _, row in matches.iterrows()]

            # Iteratory do śledzenia bieżącego pionu i narodowości
            pion_idx = 0
            narodowosc_idx = 0

            while pion_idx < len(non_zero_columns) and narodowosc_idx < len(
                    narodowosci):
                pion_name, pion_value = non_zero_columns[pion_idx]
                narodowosc_code, narodowosc_count = narodowosci[narodowosc_idx]

                # Liczba osób do wstawienia w tej iteracji
                liczba_do_wstawienia = min(pion_value, narodowosc_count)

                for _ in range(liczba_do_wstawienia):
                    rule = pion_name.replace(" ", "_")
                    obywatel_kod = narodowosc_code + generate_gender() + generate_age_group()
                    inserted_crossing.append(
                        f"INSERT INTO Fakt_Przekroczenie_granicy (Kl_przekroczenie, Kl_czas, Kl_przejscie, Kl_osoba, Kl_zasada, Kl_kierunek) "
                        f"VALUES (NEWID(), '{osoba_row['Data']}', '{osoba_row['PrzejscieKlucz']}', '{obywatel_kod}', '{rule}', '{osoba_row['Kierunek']}');"
                    )

                # Aktualizacja liczby osób i wartości pionu
                pion_value -= liczba_do_wstawienia
                narodowosc_count -= liczba_do_wstawienia

                # Jeśli pion się wyczerpał, przejdź do kolejnego
                if pion_value == 0:
                    pion_idx += 1

                # Jeśli narodowość się wyczerpała, przejdź do kolejnej
                if narodowosc_count == 0:
                    narodowosc_idx += 1


    else:

        # Znajdź kolumny z niezerowymi wartościami
        non_zero_columns = []
        # for col_name, value in osoba_row.items():
        #     if col_name not in ["Placówka SG", "Przejście", "Rodzaj przejścia",
        #                         "Odcinek", "Oddział SG", "Data", "Kto",
        #                         "Kierunek", "Razem"]:
        for col_name in ["Paszportowy", "Pozasystemowa", "MRG", "Inny",
                         "Załogi pociągów osobowych",
                         "Załogi pociągów towarowych",
                         "Załogi statków pasażerskich",
                         "Załogi statków handlowych",
                         "Załogi statków rybackich", "Załogi kutrów",
                         "Załogi taboru rzecznego",
                         "Załogi jednostek sportowo - żeglarskich",
                         "Załogi samolotów", "Załogi śmigłowców", "os.w INNYCH"
                         ]:
            try:
                value = int(osoba_row[col_name])
                numeric_value = int(value)
                if numeric_value > 0:
                    non_zero_columns.append((col_name, numeric_value))
            except ValueError:
                continue
        for col_name, count in non_zero_columns:
            # Generuj odpowiednią liczbę zapytań INSERT
            for _ in range(count):
                rule = col_name.replace(" ", "_")
                osoba_klucz = 'PL' + generate_gender() + generate_age_group()
                inserted_crossing.append(
                    f"INSERT INTO Fakt_Przekroczenie_granicy (Kl_przekroczenie, Kl_czas, Kl_przejscie, Kl_osoba, Kl_zasada, Kl_kierunek) "
                    f"VALUES (NEWID(), '{osoba_row['Data']}', '{osoba_row['PrzejscieKlucz']}', '{osoba_klucz}', '{rule}', '{osoba_row['Kierunek']}');"
                )

# Tabela Czas
unique_dates = osoby_df["Data"].drop_duplicates()
for date in unique_dates:
    dt = datetime.strptime(str(date), "%Y-%m-%d")
    insert_queries.add(
        f"INSERT INTO Czas (Kl_czas, dzien, miesiac, rok, dzien_tygodnia) VALUES ('{date}', {dt.day}, {dt.month}, {dt.year}, '{dt.strftime('%A')}');"
    )

# Tabela Przejscie_graniczne
unique_crossings = osoby_df.drop_duplicates(
    subset=["Przejście", "Rodzaj przejścia", "Odcinek", "Placówka SG",
            "Oddział SG"])
for _, row in unique_crossings.iterrows():
    insert_queries.add(
        f"INSERT INTO Przejscie_graniczne (Kl_przejscie, nazwa_przejscia ,typ_przejscia_granicznego, odcinek_z_ktorym_panstwem, nazwa_przejscia, placowka_sluzby_granicznej, oddzial_SG) "
        f"VALUES ('{row['PrzejscieKlucz']}', '{row['Przejście']}' ,'{row['Rodzaj przejścia']}', '{row['Odcinek']}', '{row['Przejście']}', '{row['Placówka SG']}', '{row['Oddział SG']}');"
    )

# Tabela Osoba
# for _, osoba_row in osoby_df.iterrows():
#     # Znajdz odpowiedni rekord w cudzoziemcy_df na podstawie kluczowych pól
#     matches = cudzoziemcy_df[(cudzoziemcy_df["Data"] == osoba_row["Data"]) &
#                              (cudzoziemcy_df["Kierunek"] == osoba_row[
#                                  "Kierunek"]) &
#                              (cudzoziemcy_df["PrzejscieKlucz"] == osoba_row[
#                                  "PrzejscieKlucz"])]
#
#     if not matches.empty:
#         for _, match_row in matches.iterrows():
#             try:
#                 for gender in ["M", "F"]:
#                     for age_group in ['0-10', '10-17', '18-27', '28-40',
#                                       '41-65',
#                                       '65-102']:
#                         querry = (
#                             f"INSERT INTO Osoba (Kl_osoba, kod_obywatelstwa ,obywatelstwo, grupa_wiekowa, plec) "
#                             f"VALUES ('{match_row['Obywatelstwo (kod)'] + gender + age_group}','{match_row['Obywatelstwo (kod)']}' ,'{match_row['Obywatelstwo (nazwa)']}', '{age_group}', '{gender}');"
#                         )
#
#                         if querry in insert_queries:
#                             raise Exception
#                         insert_queries.add(querry)
#             except Exception as e:
#                 continue


# Zbiór unikalnych kodów obywatelstwa z cudzoziemców
unique_citizenships = cudzoziemcy_df[["Obywatelstwo (kod)", "Obywatelstwo (nazwa)"]].drop_duplicates()

# Dodanie wiersza dla Polski
polska_row = pd.DataFrame([{"Obywatelstwo (kod)": "PL", "Obywatelstwo (nazwa)": "Polska"}])
unique_citizenships = pd.concat([unique_citizenships, polska_row], ignore_index=True)

# Możliwe grupy wiekowe i płcie
age_groups = ['0-10', '10-17', '18-27', '28-40', '41-65', '65-102']
genders = ['M', 'F']

# Generowanie kombinacji Osoba
for _, citizenship_row in unique_citizenships.iterrows():
    for gender in genders:
        for age_group in age_groups:
            querry = (
                f"INSERT INTO Osoba (Kl_osoba, kod_obywatelstwa, obywatelstwo, grupa_wiekowa, plec) "
                f"VALUES ('{citizenship_row['Obywatelstwo (kod)'] + gender + age_group}', '{citizenship_row['Obywatelstwo (kod)']}', '{citizenship_row['Obywatelstwo (nazwa)']}', '{age_group}', '{gender}');"
            )
            insert_queries.add(querry)

# Zapisz zapytania do pliku
output_file = "output_script.sql"
crossing_file = "crossing_script.sql"
with open(output_file, "w", encoding="utf-8") as file:
    file.write("\n".join(insert_queries))
with open(crossing_file, "w", encoding="utf-8") as file:
    file.write("\n".join())
print(f"Skrypt SQL zapisany w pliku {output_file}")
