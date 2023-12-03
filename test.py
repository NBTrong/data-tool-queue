import requests

url = "http://192.168.1.80:9200/api/v1/queue/import"  # Replace with the actual URL

payload = [{'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': '#kemchongna #kemchongnang #kemchongnangnangtone #mayeptoc #maysaytoc #daugoixabiotincollagen #daugoixa ',
            'thumb_url': 'https://p77-sign-va.tiktokcdn.com/obj/tos-maliva-p-0068/okMoTx3dAu3CGJkyIW9Ag1HnVhI6EAfQSNtKtz?x-expires=1700370000&x-signature=zoy%2Bs9JRWMrHC0adjOAi16%2BoYJw%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@myphamnguyentuoi/video/7228520734614670597', 'koc': 'myphamnguyentuoi',
            'total_comments': 0, 'total_likes': 3, 'total_saves': 1, 'total_shares': 0, 'total_views': 180,
            'uploaded_time': '2023-05-02 09:52:20'},
           {'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': 'kem chống nắng nâng tone thẩm thấu nhanh không gây bết dính dành cho các loại da.#kemchongnang #kemchongnangnangtone #kcn #xuhuong #xuhuongtiktok #kemchongnangkiemdau #kemchongnangbody #kemchongna ',
            'thumb_url': 'https://p16-sign-sg.tiktokcdn.com/obj/tos-alisg-p-0037/okis7EUPQAVi8BYB8VF8ABdANZ5IExTEEt7AT?x-expires=1700370000&x-signature=3q6RMMZb9287OgeAZqrOFG8%2FYrE%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@traoniemtin_nhangiatri/video/7294798956083662088',
            'koc': 'traoniemtin_nhangiatri', 'total_comments': 0, 'total_likes': 3, 'total_saves': 0, 'total_shares': 0,
            'total_views': 16, 'uploaded_time': '2023-10-28 00:25:44'},
           {'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': '#xuhuong #TikTokFashion #luungocyennn #kemchongnangnangtone #kemchongna ',
            'thumb_url': 'https://p16-sign-sg.tiktokcdn.com/obj/tos-alisg-p-0037/oo0EeDAngzOBgZs8bDlRBEAuQTeBgLgg2KwiMU?x-expires=1700370000&x-signature=8HjfeI5UO8V8hJDilhF0hqumW4U%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@luungocyennn/video/7291161784642194696', 'koc': 'luungocyennn',
            'total_comments': 0, 'total_likes': 2, 'total_saves': 0, 'total_shares': 0, 'total_views': 115,
            'uploaded_time': '2023-10-18 05:11:35'},
           {'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': '#CapCut không thể thiếu trước nắng hè #kemchongnangnangtone #kemchongna ',
            'thumb_url': 'https://p16-sign-va.tiktokcdn.com/obj/tos-maliva-p-0068/oA1DeJElbACZEDnfIgdcHNQg58jHPXxqySDICe?x-expires=1700370000&x-signature=nZdCSXlacquIMd8FKBiOkIMYma8%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@24062001_/video/7254870191962590470', 'koc': '24062001_',
            'total_comments': 2, 'total_likes': 2, 'total_saves': 1, 'total_shares': 0, 'total_views': 187,
            'uploaded_time': '2023-07-12 10:01:38'},
           {'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': '📍SONG JOONG KI TRỞ THÀNH ĐẠI SỨ CỦA THƯƠNG HIỆU DƯỢC MỸ PHẨM DR.G Dr.G là thương hiệu Dược mỹ phẩm hàng đầu Hàn Quốc với hơn 20 năm đồng hành cùng sức khỏe của hàng triệu làn da châu Á, đem đến các giải pháp chăm sóc da chuyên nghiệp được cá nhân hóa cho từng loại da. Cre #drg #drgxsongjongki #songjongki #drgsongjongki #kemchongna #kem_chống_nắng #kem_dưỡng #kemchốngnắng #mypham #trangđiểm #makeup #suncream #kemchốngnắngnângtone #chốngnắng #da_mụn #mụn #dr_g #redblemish #redblemishclearsoothingcream #kem_dưỡng_da #trịmụn #giấythấmdầu ',
            'thumb_url': 'https://p16-sign-va.tiktokcdn.com/obj/tos-maliva-p-0068/c83f422b340f4eaf91041ce40de6cf9f_1687879904?x-expires=1700370000&x-signature=7LN0cX6KhdWccSbhv3fxayof%2BDw%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@j96scorner/video/7249388963142601989', 'koc': 'j96scorner',
            'total_comments': 0, 'total_likes': 2, 'total_saves': 1, 'total_shares': 0, 'total_views': 333,
            'uploaded_time': '2023-06-27 15:31:41'},
           {'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': 'Kem chống nắng #kemchongna ☝🏻☝🏻☝🏻',
            'thumb_url': 'https://p16-sign-useast2a.tiktokcdn.com/obj/tos-useast2a-p-0037-aiso/oshZE9eT64PnZdAyDBnAIHMLVBkQ0BkCHtDfbb?x-expires=1700370000&x-signature=UBVJJDAjHZArN%2FM%2FaxmbdGMWFk4%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@tuyen_nguyen99/video/7220064109071437083', 'koc': 'tuyen_nguyen99',
            'total_comments': 0, 'total_likes': 2, 'total_saves': 0, 'total_shares': 0, 'total_views': 34,
            'uploaded_time': '2023-04-09 14:56:21'},
           {'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': 'Kem chống nắng hàn quốc#kemchongna #chongnang #myphamhanquoc #CapCut ',
            'thumb_url': 'https://p77-sign-va-lite.tiktokcdn.com/obj/tos-maliva-p-0068/ow0BF3UbfrBkeBAWIqqERSMnQD6EJWBkBi6sQj?x-expires=1700370000&x-signature=z4yLdDOzpkkPpBXV023GNLbYkLo%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@diem93z/video/7271988091592707333', 'koc': 'diem93z',
            'total_comments': 2, 'total_likes': 0, 'total_saves': 0, 'total_shares': 0, 'total_views': 117,
            'uploaded_time': '2023-08-27 13:07:50'},
           {'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': '#kemchongna #tiktokmakemebuyit #GamingOnTikTok #nộitrợ #gái #gaixinhtikt #phụnữ #contrai #viralvideo #tiktok #viral ',
            'thumb_url': 'https://p16-sign-va.tiktokcdn.com/obj/tos-maliva-p-0068/ocNybAnsMdlBfeRIkGMAvAJEQIulBcFQdJGrDd?x-expires=1700370000&x-signature=BbBnC4IO%2FnUT01XCFfg3wZkeFV4%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@sasa070552/video/7256068584865123589', 'koc': 'sasa070552',
            'total_comments': 0, 'total_likes': 0, 'total_saves': 0, 'total_shares': 0, 'total_views': 2,
            'uploaded_time': '2023-07-15 15:32:02'},
           {'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': '#kemchongnang #kemchongna  #chongnangkiemdau ',
            'thumb_url': 'https://p16-sign-va.tiktokcdn.com/obj/tos-maliva-p-0068/fa4162a624b74d96b12856f67ee73bfb_1688024540?x-expires=1700370000&x-signature=8fWEQRRdWfo1hvzrnhuFsqBXh%2BM%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@ngocha081080/video/7250010174008659205', 'koc': 'ngocha081080',
            'total_comments': 0, 'total_likes': 0, 'total_saves': 0, 'total_shares': 0, 'total_views': 143,
            'uploaded_time': '2023-06-29 07:42:19'},
           {'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': 'Các dòng kem chống nắng có chỉ số chống tia UV cao giúp da k bị bắt nắng#lenxuhuongtiktok #xuhuong2023 #kemchongna #kemchongnangnangtone #kemchongnangdadaumun #lamdep #lamdepkhongkho #thơitrang #phunu #phunuhienđai #taphoaonnline ',
            'thumb_url': 'https://p16-sign-va.tiktokcdn.com/tos-maliva-p-0068/ochj0DesJkC7e97XCW8Dg8neIFAjbivgQxb5S4~tplv-dmt-logom:tos-useast2a-v-0068/883660c124b048998be73152f255b533.image?x-expires=1700370000&x-signature=Qpg1DvlAxfb3KbKoA%2FqPhgVY%2B9o%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@hoaian.thanhdat.tuananh/video/7245103928096869638',
            'koc': 'hoaian.thanhdat.tuananh', 'total_comments': 0, 'total_likes': 0, 'total_saves': 0,
            'total_shares': 0, 'total_views': 173, 'uploaded_time': '2023-06-16 02:23:34'},
           {'keyword': '1665293428961282', 'platform': 'tiktok', 'input_file_id': '326',
            'description': 'Các loại kem chống nắng quốc dân #xuhuong #lamdepstore #lamdepstorevn #kemchongna',
            'thumb_url': 'https://p16-sign-sg.tiktokcdn.com/obj/tos-alisg-p-0037/ooPGPFqhtyEdwvWxAOAIKtifrtup8nBoAzJAwI?x-expires=1700370000&x-signature=JfmK6vvVuJpZmmdv0TCFyH%2Fhk6Y%3D&s=CHALLENGE_AWEME&se=false&sh=&sc=cover&l=20231118053636B02DB59A594D1B3FCF03',
            'post_url': 'https://www.tiktok.com/@me_be7/video/7171337487275576578', 'koc': 'me_be7',
            'total_comments': 0, 'total_likes': 0, 'total_saves': 0, 'total_shares': 0, 'total_views': 0,
            'uploaded_time': '2022-11-29 07:35:18'}]

params = {
    "tab": "comments-extract-data"
}

response = requests.post(url, json=payload, params=params)

if response.status_code == 200:
    print("POST request successful")
    print("Response:", response.json())
else:
    print(f"POST request failed with status code {response.status_code}")
    print("Response:", response.text)
