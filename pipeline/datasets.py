import xarray as xr
# UH = 
# downwhatever = 
#Takes the dataset with uwind and vwind and generates dataset with wind
def make_max_wind_dt(dt):
    # print("making max wind dt")
    wind_dt = xr.Dataset(
            data_vars=dict(
                max_wind=(["ygrid_0", "xgrid_0"], 
                      ((dt.MAXUW_P8_L103_GLC0_max1h.data)**2 + (dt.MAXVW_P8_L103_GLC0_max1h.data)**2)**(1/2)),
                land=(["ygrid_0", "xgrid_0"],
                     dt.LAND_P0_L1_GLC0.data)
            ),
            coords=dict(
                gridlat_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlat_0'].data),
                gridlon_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlon_0'].data)
            )
    )
                
    return wind_dt

#Takes the dataset with uwind and vwind and generates dataset with wind
def make_wind_dt(dt):
    wind_dt = xr.Dataset(
            data_vars=dict(
                wind=(["ygrid_0", "xgrid_0"], 
                      ((dt.UGRD_P0_L103_GLC0.data)**2 + (dt.VGRD_P0_L103_GLC0.data)**2)**(1/2)),
                land=(["ygrid_0", "xgrid_0"],
                     dt.LAND_P0_L1_GLC0.data)
            ),
            coords=dict(
                gridlat_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlat_0'].data),
                gridlon_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlon_0'].data)
            )
    )
                
    return wind_dt
                
def make_downdraft_dt(dt):
    down_draft_dt = xr.Dataset(
            data_vars=dict(
                max_downdraft=(["ygrid_0", "xgrid_0"], 
                      (dt.MAXDVV_P8_2L100_GLC0_max1h.data)),
                land=(["ygrid_0", "xgrid_0"],
                     dt.LAND_P0_L1_GLC0.data)
            ),
            coords=dict(
                gridlat_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlat_0'].data),
                gridlon_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlon_0'].data)
            )
    )

    return down_draft_dt

def make_gust_dt(dt):
    gust_dt = xr.Dataset(
            data_vars=dict(
                gust=(["ygrid_0", "xgrid_0"], 
                      (dt.GUST_P0_L1_GLC0.data)),
                land=(["ygrid_0", "xgrid_0"],
                     dt.LAND_P0_L1_GLC0.data)
            ),
            coords=dict(
                gridlat_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlat_0'].data),
                gridlon_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlon_0'].data)
            )
    )
    return gust_dt
    
def make_25_UH_dt(dt):
    uh_25_dt = xr.Dataset(
            data_vars=dict(
                uh_25=(["ygrid_0", "xgrid_0"], 
                      (dt.MXUPHL25_P8_2L103_GLC0_max1h.data)),
                land=(["ygrid_0", "xgrid_0"],
                     dt.LAND_P0_L1_GLC0.data)
            ),
            coords=dict(
                gridlat_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlat_0'].data),
                gridlon_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlon_0'].data)
            )
    )
    return uh_25_dt
    
def make_03_UH_dt(dt):
    uh_03_dt = xr.Dataset(
            data_vars=dict(
                uh_03=(["ygrid_0", "xgrid_0"], 
                      (dt.MXUPHL03_P8_2L103_GLC0_max1h.data)),
                land=(["ygrid_0", "xgrid_0"],
                     dt.LAND_P0_L1_GLC0.data)
            ),
            coords=dict(
                gridlat_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlat_0'].data),
                gridlon_0=(["ygrid_0", "xgrid_0"], dt.coords['gridlon_0'].data)
            )
    )

    return uh_03_dt