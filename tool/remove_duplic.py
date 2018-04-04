import os

output_file_tsv = "douban_movie_temp.tsv"

header_list = ['#scraper01',
               'title_movie',
               'title_movie_original',
               'URL_image',
               'date_year_release',
               'date_release_bycountry_csv',
               'category',
               'date_time_runtime_bycountry_csv',
               'address_country_producing',
               'title_movie_bycountry_csv',
               'language_csv',
               'name_actor_csv',
               'name_director_csv',
               'name_screenwriter_csv',
               'number_rating',
               'number_raters',
               'URL_homepage',
               'URL_imdb',
               'description'
               ]
HEADER = '\t'.join(header_list) + '\n'

with open(output_file_tsv, encoding='utf-8') as open_file:
    lines = open_file.readlines()

line_set = set(lines)

with open(output_file_tsv, 'w', encoding='utf-8') as open_file:
    open_file.write(HEADER)
    open_file.writelines(line_set)

command = 'gzip -f ' + output_file_tsv
print('command = ', command)
os.system(command)
