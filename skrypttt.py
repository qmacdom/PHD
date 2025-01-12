import codecs


def split_file(input_file, lines_per_file):
    # Otwórz plik wejściowy w trybie odczytu z kodowaniem UTF-8
    with open(input_file, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Liczba linii w pliku
    total_lines = len(lines)

    # Obliczamy, ile plików będzie potrzebnych
    num_files = (
                            total_lines + lines_per_file - 1) // lines_per_file  # Zaokrąglenie w górę

    # Tworzymy pliki wyjściowe
    for i in range(num_files):
        start_line = i * lines_per_file
        end_line = min((i + 1) * lines_per_file, total_lines)

        # Zapisz odpowiedni fragment do nowego pliku
        output_file = f"{input_file}_part_{i + 1}.txt"
        with open(output_file, 'w', encoding='utf-8') as outfile:
            outfile.writelines(lines[start_line:end_line])

        print(
            f"Stworzono plik: {output_file} ({start_line + 1} do {end_line})")


# Przykład użycia
split_file('crossing_script.sql', 450000)
