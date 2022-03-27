#%%
from extract_sds import get_rss_data
import re
# %%
data = get_rss_data()

# %%
data['episde_link'] = data['content'].apply(lambda x: get_weblink(x))

data.head()
# %%
