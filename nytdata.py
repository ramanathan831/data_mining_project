from pynytimes import NYTAPI

nyt = NYTAPI("Type_Your_Key")

reviews = nyt.movie_reviews(keyword = "Batman")

l=['display_title','mpaa_rating','headline','summary_short']
review_length=len(reviews)
for u in range(review_length):
    str1=""
    for i in l:
        s=reviews[u][i]
        str1+=s+"\t"
    print(str1)

# print(reviews)


