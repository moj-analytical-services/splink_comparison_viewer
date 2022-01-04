# splink_comparison_viewer


## Understanding the tool

There's a tutorial video available [here](https://www.youtube.com/watch?v=DNvCMqjipis).

## Usage

To generate a dashboard:

```python

from splink import Splink
linker = Splink(settings_obj.settings_dict, df, spark)
df_e = linker.get_scored_comparisons()


from splink_comparison_viewer import get_vis_data, render_html_vis
edges_data = get_vis_data(df_e, linker.model.current_settings_obj.settings_dict, 3)
render_html_vis(edges_data, linker.model.current_settings_obj.settings_dict, "out.html", True)

```

For big `df_e`, probably good to save it out to disk before passing to `get_vis_data()`

For very big `df_e` with a large number of distinct comparison vector patterns (>20k), might want to filter down `edges_data` before passing to `render_html_vis` e.g. to remove entries with low counts.

## Example

Example output [here](https://www.robinlinacre.com/splink_example_charts/example_charts/splink_comparison_viewer.html)


![image](https://user-images.githubusercontent.com/2608005/143070560-9e51e6a1-4187-45fe-b524-6e7d2437d765.png)
