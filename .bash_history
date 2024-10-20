rm .bash_history
git rm --cached .bash_history
git ls-files
git show 9102d764135099138389d4043c2b1a7c6e50d237
git log --oneline
git ls-files | grep bash_history
git filter-repo --path .bash_history --invert-paths
pip install git-filter-repo
git filter-repo --path .bash_history --invert-paths
python3 -m git_filter_repo --path .bash_history --invert-paths
git push origin main --force
git remote -v
git remote add origin https://github.com/sckintas/airflow_gcp_project.git
git remote -v
git push origin main --force
