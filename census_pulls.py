#%% weighted average income


def census_pull(tables,geom,year='2019',yearrange='5',key="6c14346c155c7ae9110a833a854582dc60c3afd0"):
    import pandas as pd
    from functools import reduce
    print("Pulling data from Census API.........")
    try:
        if type(yearrange) != str:
            yearrange = str(yearrange)

        if yearrange != '5':
            if yearrange != '1':
                print("Unable to continue; year range incorrect")
                return

        if type(year) != str:
            year = str(year)

        if type(tables) != list:
            tables = [tables]
            
        frames = []
        if year == '2000':
            for a in tables:
                print("Working on table {a}...".format(a=a))
                try:
                    if geom == 'tract':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=tract:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census.loc[:,'state'].str.rjust(2,"0") + df_census.loc[:,'county'].str.rjust(3,"0") + df_census.loc[:,'tract'].str.ljust(6,"0")
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    
                    elif geom == 'zcta':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=zip%20code%20tabulation%20area:*&in=state:48&key={key}"
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['zip code tabulation area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    
                    elif geom == 'bgs':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=block%20group:*&in=state:48%20county:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['state'] + df_census['county'] + df_census['tract'] + df_census['block group']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))

                    elif geom == 'puma':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=public%20use%20microdata%20area:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['public use microdata area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))

                    elif geom == 'place':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=place:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['state'] + df_census['place']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    elif geom == 'county':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=county:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['county']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    elif geom == 'msa':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['metropolitan statistical area/micropolitan statistical area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                except:
                    print(f"Error on {a}")
                            
                if len(tables) > 1:
                    cdf = reduce(lambda x,y: pd.merge(x,y, on='GEOID', how='inner', suffixes=('', '_drop')), frames)
                    cdf.drop([col for col in cdf.columns if 'drop' in col], axis=1, inplace=True)
                else:
                    cdf = frames[0]
                cdf.fillna(0,inplace=True)

                #to numeric 
                icol = [s for s in cdf.columns if any(xs[:6] in s for xs in tables)]
                #replace common nulls
                cdf.loc[:,icol] = cdf.loc[:,icol].replace({'-666666666':'0','-222222222':'0',None:'0','None':'0'})
                #get estimates only
                icol_e = [s for s in icol if s.endswith('E')]
                #return as numeric
                cdf.loc[:,icol_e] = cdf.loc[:,icol_e].astype(float)
                print("Done")
                return cdf
        else:  
            for a in tables: 
                print("Working on table {a}...".format(a=a))
                try:
                    if geom == 'tract':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=tract:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census.loc[:,'state'] + df_census.loc[:,'county'] + df_census.loc[:,'tract']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    
                    elif geom == 'zcta':
                        if year == '2020':
                            apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=zip%20code%20tabulation%20area:*&key=" + key
                        else:
                            apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=zip%20code%20tabulation%20area:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['zip code tabulation area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    
                    elif geom == 'bgs':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=block%20group:*&in=state:48%20county:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['state'] + df_census['county'] + df_census['tract'] + df_census['block group']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))

                    elif geom == 'puma':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=public%20use%20microdata%20area:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['public use microdata area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))

                    elif geom == 'place':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=place:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['state'] + df_census['place']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    elif geom == 'county':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=county:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['county']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    elif geom == 'msa':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['metropolitan statistical area/micropolitan statistical area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                except:
                    print(f"Error on {a}")
                            
            if len(tables) > 1:
                cdf = reduce(lambda x,y: pd.merge(x,y, on='GEOID', how='inner', suffixes=('', '_drop')), frames)
                cdf.drop([col for col in cdf.columns if 'drop' in col], axis=1, inplace=True)
            else:
                cdf = frames[0]
            cdf.fillna(0,inplace=True)

            #to numeric 
            icol = [s for s in cdf.columns if any(xs[:6] in s for xs in tables)]
            #replace common nulls
            cdf.loc[:,icol] = cdf.loc[:,icol].replace({'-666666666':'0','-222222222':'0',None:'0','None':'0'})
            #get estimates only
            icol_e = [s for s in icol if s.endswith('E')]
            #return as numeric
            cdf.loc[:,icol_e] = cdf.loc[:,icol_e].astype(float)
            print("Done")
            return cdf
    except:
        print("Error running census pull")

def variable_lookup(year=2019,fivoroneyr=5):
    try:
        from bs4 import BeautifulSoup
        import requests
        import pandas as pd
    except:
        print("Error on library import...")

    try:
        #create variable table
        if int(year) > 2000:
            print("Requesting...")
            a = requests.get(r"https://api.census.gov/data/{y}/acs/acs{q}/variables.html".format(y=year,q=fivoroneyr))
        elif int(year) == 2000:
            a = requests.get(r"https://api.census.gov/data/2000/dec/sf3/variables.html")
        page = a.text

        print("Souping and parsing...")
        soup = BeautifulSoup(page, "html.parser")
        found = soup.find_all("tr")
        
        print("Cleaning...")
        #get column names
        cols = []
        for z in soup.find_all("thead"):
            names = z.findChildren('th')
            for cell in names:
                name_2 = cell.string
                cols.append(name_2)
        
        #get values
        data = []
        for row in found:
            vars = []
            cells = row.findChildren('td')
            for cell in cells:
                value = cell.string
                vars.append(value)
            z = [str(y) for y in vars]
            data.append(z)
        
        #make a df
        lookup = pd.DataFrame(data=data,columns=cols)
        lookup = lookup.fillna("None")
    except:
        print("Error on lookup...")
    print("Done")
    return lookup

def race_table_clean(df):
    import pandas as pd
    from functools import reduce
    print("Cleaning race data...")
    df['total_race_all'] = df['B03002_001E']
    df['white_nh'] = df['B03002_003E']
    df['black_afam_both'] = df['B03002_004E'] + df['B03002_014E']
    df['amin_both'] = df['B03002_005E'] + df['B03002_015E'] 
    df['asian_both'] = df['B03002_006E'] + df['B03002_016E']
    df['nat_haw_both'] = df['B03002_007E'] + df['B03002_017E']
    df['other_nh'] = df['B03002_008E'] 
    df['two_or_more_both'] = df['B03002_011E'] + df['B03002_021E']
    df['white_h'] = df['B03002_013E']
    df['some_other_h'] = df['B03002_018E']
    df['hisp_est'] = df['white_h'] + df['some_other_h']
    df['hisp_total'] = df['B03002_012E']
    df['nonhisp_total'] = df['B03002_002E']
    df.drop(columns=['B03002_001E','B03002_002E','B03002_003E','B03002_004E','B03002_005E','B03002_006E','B03002_007E','B03002_008E',
    'B03002_009E','B03002_010E','B03002_011E','B03002_012E','B03002_013E','B03002_014E','B03002_015E','B03002_016E','B03002_017E','B03002_018E','B03002_019E','B03002_020E','B03002_021E'],inplace=True)
    return df

def edu_table_clean(df):
    import pandas as pd
    from functools import reduce
    print("Cleaning education data")
    df['tot_edu'] = df.loc[:,'B15003_001E']
    df['below_hs'] = df.loc[:,['B15003_002E','B15003_003E','B15003_004E','B15003_005E','B15003_006E','B15003_007E','B15003_008E',
    'B15003_009E','B15003_010E','B15003_011E','B15003_012E','B15003_013E','B15003_014E','B15003_015E','B15003_016E']].sum(axis=1)
    df['hs_dipl'] = df.loc[:,['B15003_017E','B15003_018E']].sum(axis=1)
    df['some_coll'] = df.loc[:,['B15003_019E','B15003_020E']].sum(axis=1)
    df['assoc'] = df.loc[:,'B15003_021E']
    df['bach'] = df.loc[:,'B15003_022E']
    df['above bach'] = df.loc[:,['B15003_023E','B15003_024E','B15003_025E']].sum(axis=1)
    df.drop(columns=['B15003_001E','B15003_002E','B15003_003E','B15003_004E','B15003_005E','B15003_006E','B15003_007E','B15003_008E','B15003_009E','B15003_010E',
    'B15003_011E','B15003_012E','B15003_013E','B15003_014E','B15003_015E','B15003_016E','B15003_017E','B15003_018E','B15003_019E','B15003_020E','B15003_021E','B15003_022E','B15003_023E','B15003_024E','B15003_025E'],inplace=True)
    return df


def census_pull_nation(tables,geom,year='2019',yearrange='5',key="6c14346c155c7ae9110a833a854582dc60c3afd0"):
    import pandas as pd
    from functools import reduce
    print("Pulling data from Census API.........")
    try:
        if type(yearrange) != str:
            yearrange = str(yearrange)

        if yearrange != '5':
            if yearrange != '1':
                print("Unable to continue; year range incorrect")
                return

        if type(year) != str:
            year = str(year)

        if type(tables) != list:
            tables = [tables]
            
        frames = []
        if year == '2000':
            for a in tables:
                print("Working on table {a}...".format(a=a))
                try:
                    if geom == 'tract':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=tract:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census.loc[:,'state'].str.rjust(2,"0") + df_census.loc[:,'county'].str.rjust(3,"0") + df_census.loc[:,'tract'].str.ljust(6,"0")
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    
                    elif geom == 'zcta':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=zip%20code%20tabulation%20area:*&in=state:48&key={key}"
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['zip code tabulation area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    
                    elif geom == 'bgs':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=block%20group:*&in=state:48%20county:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['state'] + df_census['county'] + df_census['tract'] + df_census['block group']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))

                    elif geom == 'puma':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=public%20use%20microdata%20area:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['public use microdata area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))

                    elif geom == 'place':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=place:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['state'] + df_census['place']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    elif geom == 'county':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=county:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['county']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    elif geom == 'msa':
                        apiaddress = f"https://api.census.gov/data/2000/dec/sf3?get=NAME,group({a})&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['metropolitan statistical area/micropolitan statistical area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                except:
                    print(f"Error on {a}")
                            
                if len(tables) > 1:
                    cdf = reduce(lambda x,y: pd.merge(x,y, on='GEOID', how='inner', suffixes=('', '_drop')), frames)
                    cdf.drop([col for col in cdf.columns if 'drop' in col], axis=1, inplace=True)
                else:
                    cdf = frames[0]
                cdf.fillna(0,inplace=True)

                #to numeric 
                icol = [s for s in cdf.columns if any(xs[:6] in s for xs in tables)]
                #replace common nulls
                cdf.loc[:,icol] = cdf.loc[:,icol].replace({'-666666666':'0','-222222222':'0',None:'0','None':'0'})
                #get estimates only
                icol_e = [s for s in icol if s.endswith('E')]
                #return as numeric
                cdf.loc[:,icol_e] = cdf.loc[:,icol_e].astype(float)
                print("Done")
                return cdf
        else:  
            for a in tables: 
                print("Working on table {a}...".format(a=a))
                try:
                    if geom == 'tract':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=tract:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census.loc[:,'state'] + df_census.loc[:,'county'] + df_census.loc[:,'tract']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    
                    elif geom == 'zcta':
                        if year == '2020':
                            apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=zip%20code%20tabulation%20area:*&key=" + key
                        else:
                            apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=zip%20code%20tabulation%20area:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['zip code tabulation area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    
                    elif geom == 'bgs':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=block%20group:*&in=state:48%20county:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['state'] + df_census['county'] + df_census['tract'] + df_census['block group']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))

                    elif geom == 'puma':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=public%20use%20microdata%20area:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['public use microdata area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))

                    elif geom == 'place':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=place:*&in=state:48&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['state'] + df_census['place']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    elif geom == 'county':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=county:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['county']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                    elif geom == 'msa':
                        apiaddress = f"https://api.census.gov/data/{year}/acs/acs{yearrange}?get=NAME,group(" + a + ")&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key=" + key
                        try:
                            df_census = pd.read_json(apiaddress)
                            df_census.columns = df_census.iloc[0]
                            df_census = df_census.reindex(df_census.index.drop(0)).reset_index(drop=True)
                            df_census['GEOID'] = df_census['metropolitan statistical area/micropolitan statistical area']
                            frames.append(df_census)
                        except:
                            print("Error on {tn}".format(tn=a))
                except:
                    print(f"Error on {a}")
                            
            if len(tables) > 1:
                cdf = reduce(lambda x,y: pd.merge(x,y, on='GEO_ID', how='inner', suffixes=('', '_drop')), frames)
                cdf.drop([col for col in cdf.columns if 'drop' in col], axis=1, inplace=True)
            else:
                cdf = frames[0]
            cdf.fillna(0,inplace=True)

            #to numeric 
            icol = [s for s in cdf.columns if any(xs[:6] in s for xs in tables)]
            #replace common nulls
            cdf.loc[:,icol] = cdf.loc[:,icol].replace({'-666666666':'0','-222222222':'0',None:'0','None':'0'})
            #get estimates only
            icol_e = [s for s in icol if s.endswith('E')]
            #return as numeric
            cdf.loc[:,icol_e] = cdf.loc[:,icol_e].astype(float)
            print("Done")
            return cdf
    except:
        print("Error running census pull")

#%%
