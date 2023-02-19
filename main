#projekt Julia Grzegorowska i Angelika Bulkowska
#Data Science, II semestr

import pandas as pd
import os

from flask import Flask, request, redirect, render_template, redirect, url_for, json
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Float, DateTime

from werkzeug.utils import secure_filename
from pathlib import Path
from hurry.filesize import size

from typing import List
from pydantic import BaseModel
import matplotlib.pyplot as plt

from werkzeug.exceptions import HTTPException
import shutil
import sys


foldery = ["histogramy", "wrzucone_pliki"]

for i in foldery:
    if not os.path.exists(i):
        os.makedirs(i)

cwd = os.getcwd()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///"+cwd+"\\baza.db"
db = SQLAlchemy(app)


###klasy
###
class dane_pliku(db.Model):
    __tablename__ = 'dane_pliku'
    id = Column(Integer, primary_key=True)
    file_name = Column(String(80), unique=True, nullable=True)
    file_size = Column(String(120), nullable=True)
    typy = Column(String(200), nullable=True)
    columns = Column(Integer, nullable=True)
    rows = Column(Integer, nullable=True)
    records_nmbr = Column(Integer, nullable=True)

    def __repr__(self):
        return f'<Nazwa_pliku: {self.file_name},\
            Rozmiar_pliku: {self.numery},\
            Typy: {self.typy},\
            Ilosc_kolumn: {self.columns},\
            Ilosc_wierszy: {self.rows},\
            Ilosc_rekordow: {self.records_nmbr}>'


class daneD(db.Model):
    __tablename__ = 'daneD'
    id = Column(Integer, primary_key=True)
    file_name = Column(String (80), nullable = True)
    
    column_name = Column(String(80), nullable=True)
    dtype = Column(String(50), nullable=True)

    # object
    unique_values = Column(Integer, nullable=True)
    empty_values = Column(Integer, nullable=True)
    nan_values = Column(Integer, nullable=True)

    # int
    min_value = Column(Float, nullable=True)
    mean_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)
    median_value = Column(Float, nullable=True)
    std_dev_value = Column(Float, nullable=True)

    # date

    min_date = Column(DateTime, nullable=True)
    max_date = Column(DateTime, nullable=True)

    def __repr__(self):
        return f'<Nazwa_pliku: {self.file_name},\
            Typ_danych: {self.dtype},\
            Nazwa_kolumny: {self.column_name},\
            Wartosci_unikalne: {self.unique_values},\
            Puste_wartosci: {self.empty_values},\
            Wartosci_NaN: {self.nan_values},\
            Wartosc_MIN: {self.min_value},\
            Wartosc_MAX: {self.max_value},\
            Wartosc_srednia:{self:mean_value},\
            Mediana: {self.median_value},\
            Odch_std: {self.std_dev_value}>,\
            Min_data: {self.min_date},\
            Max_data: {self.max_date}>'

###
###



@app.errorhandler(HTTPException)
def handle_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    response.data = json.dumps({
        "code": e.code,
        "name": e.name,
        "description": e.description,
    })
    response.content_type = "application/json"
    return response


@app.route('/')
def przenies():
    return redirect(url_for('pliki'))


###upload plików
###

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['plik']
        dataset = pd.read_csv(f, sep=';|,', engine='python')
        r, c = dataset.shape
        if r > 1000 or c > 20:
            return render_template('error_1.html')       
        else:
            filename = secure_filename(f.filename)
            name_with_dir = os.path.join('wrzucone_pliki', filename)
            try:
                open(name_with_dir, 'r')
                return render_template ('error_2.html')

                        
            except:
                f.save(name_with_dir)
                dataset.to_csv(r'wrzucone_pliki/{}'.format(filename), sep=';')
                path = cwd + "\wrzucone_pliki"
                pliki_rozmiary = {}
                liczba_rekordow = pd.read_csv(cwd + '\wrzucone_pliki\{}'.format(filename), sep=';|,',
                                              engine='python').size

                
                rows, columns = pd.read_csv(cwd + '\wrzucone_pliki\{}'.format(filename), sep=';|,',
                                            engine='python').shape
                pliki_rozmiary[filename] = {'rozmiar_pliku': size(Path(
                    cwd + '\wrzucone_pliki\{}'.format(
                        filename)).stat().st_size), 'kolumny': columns, 'wiersze': rows,
                                            'liczba_rekordow': liczba_rekordow}
                for key in pliki_rozmiary.keys():
                    file_name = key
                    file_size = pliki_rozmiary[key]['rozmiar_pliku']
                    columns = pliki_rozmiary[key]['kolumny']
                    rows = pliki_rozmiary[key]['wiersze']
                    records_nmbr = str(pliki_rozmiary[key]['liczba_rekordow'])


                    a= []
                    for col in dataset.columns:
                        typ = str(dataset[col].dtypes)
                        if typ in a:
                            pass
                        else:
                            a.append(typ)
                            
                    for col in dataset.columns:
                        if "data" in col or "date" in col or dataset[col].dtypes == "datetime64":
                            if "datatime64" in a:
                                pass
                            else:
                                a.append("datatime64")

                    b = "  ".join(a)
                    typy=str(b)
                    
                    dane = dane_pliku(file_name=file_name, file_size=file_size, typy=typy, columns=columns, rows=rows,
                                      records_nmbr=records_nmbr)
                    db.session.add(dane)
                    try:
                        db.session.commit()
                    except:
                        pass

###statystyki dla typów danych
###
                obliczenia = {}
                wartosci_puste = {}
                wartosci_licznik = {}
                
                typy_wszystkie = dataset.dtypes

                for col in dataset.columns:
                    if "data" in col or "date" in col or dataset[col].dtypes == "datetime64":
                        try:
                            c = pd.to_datetime(dataset[col])
                            min_date = min(c)
                            max_date = max(c)
                            typy = "datetime64"
                            obliczenia = daneD(file_name='{}'.format(filename),column_name=col, dtype=str(typy), min_date=min_date,
                                              max_date=max_date)
                            db.session.add(obliczenia)
                            db.session.commit()
                        except:
                            typy = "datetime64"
                            obliczenia = daneD(file_name='{}'.format(filename),column_name=col, dtype=typy+"  Błędna wartość w kolumnie - sprawdź dane !!!", min_date= None,
                                              max_date= None)
                            db.session.add(obliczenia)
                            db.session.commit()
                        
                        
                    elif dataset[col].dtypes == "object":
                        wartosci_licznik[col] = dataset[col].size
                        describe = dataset[col].describe()
                        describe['unique':'unique']
                        wartosci_unikatowe = dataset[col].nunique()
                        typy = dataset[col].dtypes

                        wartosci_puste = (dataset[col] == '').sum()
                        wartosci_nan = dataset[col].isnull().sum(axis=0)

                        obliczenia = daneD(file_name='{}'.format(filename),column_name=col, dtype=str(typy),
                                          unique_values=int(wartosci_unikatowe),
                                          empty_values=int(wartosci_puste), nan_values=int(wartosci_nan))

                        db.session.add(obliczenia)
                        db.session.commit()
                    elif dataset[col].dtypes == "int64" or dataset[col].dtypes == "float64":
                
                        try:
                            opis = dataset[col].head().describe()
                            typy = dataset[col].dtypes
                            minimalna = dataset[col].min()
                            srednia = dataset[col].mean()
                            maksymalna = dataset[col].max()
                            mediana = dataset[col].median()
                            odchylenie_standardowe = opis["std":"std"]

                            obliczenia = daneD(file_name='{}'.format(filename),column_name= col, dtype=str(typy),
                                              min_value=minimalna, mean_value=srednia,
                                              max_value=maksymalna, median_value=mediana,
                                              std_dev_value=odchylenie_standardowe)
                            db.session.add(obliczenia)
                            db.session.commit()
                        except:
                            typy = "datetime64"
                            obliczenia = daneD(file_name='{}'.format(filename),column_name= col, dtype=typy+"  Błędna wartość w kolumnie - sprawdź dane !!!",
                                              min_value=None, mean_value=None,
                                              max_value=None, median_value=None,
                                              std_dev_value=None)
                            db.session.add(obliczenia)
                            db.session.add(obliczenia)
                            db.session.commit()


        ### histogramy
        ###
                    kolumny_do_histogramu = []
                    path = cwd + '\histogramy\{}'.format(filename)
                try:
                    os.mkdir(path)
                except FileExistsError:
                    pass
                for i, j in dataset.dtypes.items():
                    if i == 'id':
                        pass
                    else:
                        try:
                            if j in ('int64', 'float64', 'category', 'Bool'):
                                kolumny_do_histogramu.append(i)
                        except TypeError:
                            pass
                for i in kolumny_do_histogramu:
                    dataset[i].hist()
                    plt.savefig(r'histogramy/{}/{}_.png'.format(filename, i))
                    plt.clf()
                return redirect(f'/saved_file/{filename}')
    elif request.method == 'GET':
        return render_template('upload.html')





### sukces uploadu plików
###

@app.route('/saved_file/<filename>')
def saved_file(filename):
    return redirect(url_for('pliki'))

### lista plików
###

@app.route('/pliki')
def pliki():
    try:
        path = cwd + "\wrzucone_pliki"
        dict_of_files = {}
        for f in os.listdir(path):
            dict_of_files[f] = path + '\{}'.format('') + f
        return render_template("list.html", dict_of_files=dict_of_files)
    except:
        return render_template("list.html", dict_of_files=dict_of_files)


### usuwanie plikow z listy
###
@app.route('/delete_data/<string:nazwa_pliku>')
def delete_data(nazwa_pliku):
    for tabela in db.metadata.tables.keys():

        a = 'db.session.query({})'.format(tabela)
        b = 'filter({}.file_name=="{}").delete()'.format(tabela, nazwa_pliku)
        c = a + '.' + b
        exec(c)
        db.session.commit()

    try:
        shutil.rmtree(cwd + '\histogramy\{}'.format(nazwa_pliku))
        os.remove(cwd + '\wrzucone_pliki\{}'.format(nazwa_pliku))    
    except:
        pass
    return render_template("delete.html")




@app.route('/pliki/<string:nazwa_pliku>')
def wyswietl_dane(nazwa_pliku):
    path = os.getcwd() + "\wrzucone_pliki"
    lista_plikow = []
    for f in os.listdir(path):
        lista_plikow.append(f)
    slownik_query = {}
    for tabela in db.metadata.tables.keys():
        lista_plikow = []
        for f in os.listdir(path):
            lista_plikow.append(f)
        a = 'db.session.query({})'.format(tabela)
        b = 'filter({}.file_name=="{}")'.format(tabela, nazwa_pliku)
        c = a + '.' + b
        slownik_query[tabela] = eval(c)
    if nazwa_pliku in lista_plikow:
        return render_template("list_data.html", slownik_query=slownik_query)
    else:
        return render_template ("error_3.html".format(nazwa_pliku))

                


if __name__ == "__main__":
    db.create_all()
    app.run(debug=False, port=1234)
