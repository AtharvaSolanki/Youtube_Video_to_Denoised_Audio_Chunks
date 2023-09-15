from pytube import YouTube
import moviepy.editor as mp
import pandas as pd
from tinytag import TinyTag
import os
from pydub import AudioSegment
from pydub.silence import split_on_silence
from multiprocessing import Pool, cpu_count
from IPython import display as disp
from IPython.display import Audio
import torch
import torchaudio
from denoiser import pretrained
from denoiser.dsp import convert_audio
import matplotlib.pyplot as plt

data=pd.read_excel("testing.xlsx")

link= data['Video Link 1']
link2=data["Video Link2"]#tst
link3=data['Video Link 3']
link4=data["Video Link 4"]
link5=data['Video Link 5']
Language=data["Language"]
Label=data["Data Label ( Male, Female, etc)"]

channel_name=[]
channel_URL=[]
video_link_1=[]
video_link_2=[]
video_link_3=[]
video_link_4=[]
video_link_5=[]
speaker_name=[]
speaker_language=[]
speaker_gender=[]
audio_offset=[] #  number of bytes before audio data begins
bitdepth=[]  # bit depth for lossless audio
Bitrate=[]
duration=[]
file_size=[]
sample_rate=[]
duration_after_chunks=[]
lin=0
a=0
for i in link:
  a=str(a)
  yt=YouTube(i)
  channel_name.append(yt.author)
  channel_URL.append(yt.channel_url)
  video_link_1.append(i)
  youtubeObject = yt.streams.get_lowest_resolution()
  youtubeObject.download(filename=f"speaker.mp4")
  clip = mp.VideoFileClip(r"speaker.mp4")
  clip.audio.write_audiofile(r"speaker_.wav")
  sound=AudioSegment.from_wav("speaker_.wav")
  os.remove("speaker.mp4")

  lang=Language[lin]
  label=Label[lin]
  lk2=link2[lin]
  lk3=link3[lin]
  lk4=link4[lin]
  lk5=link5[lin]
  video_link_2.append(lk2)
  video_link_3.append(lk3)
  video_link_4.append(lk4)
  video_link_5.append(lk5)
  speaker_language.append(lang)
  speaker_gender.append(label)
  if pd.isna(lk2):
    print("no value in link 2")
  else:
    yt=YouTube(lk2)
    youtubeObject = yt.streams.get_lowest_resolution()
    youtubeObject.download(filename=f"speaker1.mp4")
    clip = mp.VideoFileClip(r"speaker1.mp4")
    clip.audio.write_audiofile(r"speaker1_.wav")
    sound1=AudioSegment.from_wav("speaker1_.wav")
    os.remove("speaker1.mp4")

  if pd.isna(lk3):
    print("no value in link 3")
  else:
    yt=YouTube(lk3)
    youtubeObject = yt.streams.get_lowest_resolution()
    youtubeObject.download(filename=f"speaker2.mp4")
    clip = mp.VideoFileClip(r"speaker2.mp4")
    clip.audio.write_audiofile(r"speaker2_.wav")
    sound2=AudioSegment.from_wav("speaker2_.wav")
    os.remove("speaker2.mp4")

  if pd.isna(lk4):
    print("no value in link 4")
  else:
    yt=YouTube(lk4)
    youtubeObject = yt.streams.get_lowest_resolution()
    youtubeObject.download(filename=f"speaker3.mp4")
    clip = mp.VideoFileClip(r"speaker3.mp4")
    clip.audio.write_audiofile(r"speaker3_.wav")
    sound3=AudioSegment.from_wav("speaker3_.wav")
    os.remove("speaker3.mp4")


  if pd.isna(lk5):
    print("no value in link 5")
  else:
    yt=YouTube(lk5)
    youtubeObject = yt.streams.get_lowest_resolution()
    youtubeObject.download(filename=f"speaker4.mp4")
    clip = mp.VideoFileClip(r"speaker4.mp4")
    clip.audio.write_audiofile(r"speaker4_.wav")
    sound4=AudioSegment.from_wav("speaker4_.wav")
    os.remove("speaker4.mp4")

  if os.path.isfile("speaker_.wav") and os.path.isfile("speaker1_.wav") and os.path.isfile("speaker2_.wav") and os.path.isfile("speaker3_.wav") and os.path.isfile("speaker4_.wav"):
    combined= sound + sound1 + sound2 +sound3 + sound4
    file_handle = combined.export("speaker_"+a+".wav", format="wav")
    os.remove("speaker_.wav")
    os.remove("speaker1_.wav")
    os.remove("speaker2_.wav")
    os.remove("speaker3_.wav")
    os.remove("speaker4_.wav")

  elif os.path.isfile("speaker_.wav") and os.path.isfile("speaker1_.wav") and os.path.isfile("speaker2_.wav") and os.path.isfile("speaker3_.wav"):
    combined= sound + sound1 + sound2 +sound3
    file_handle = combined.export("speaker_"+a+".wav", format="wav")
    os.remove("speaker_.wav")
    os.remove("speaker1_.wav")
    os.remove("speaker2_.wav")
    os.remove("speaker3_.wav")

  elif os.path.isfile("speaker_.wav") and os.path.isfile("speaker1_.wav") and os.path.isfile("speaker2_.wav"):
    combined= sound + sound1 + sound2
    file_handle = combined.export("speaker_"+a+".wav", format="wav")
    os.remove("speaker_.wav")
    os.remove("speaker1_.wav")
    os.remove("speaker2_.wav")

  elif os.path.isfile("speaker_.wav") and os.path.isfile("speaker1_.wav") :
     combined= sound + sound1
     file_handle = combined.export("speaker_"+a+".wav", format="wav")
     os.remove("speaker_.wav")
     os.remove("speaker1_.wav")

  else:
    combined= sound
    file_handle = combined.export("speaker_"+a+".wav", format="wav")
    os.remove("speaker_.wav")

  audio = TinyTag.get("speaker_"+a+".wav")
  Bitrate.append(audio.bitrate)
  audio_offset.append(audio.audio_offset)
  bitdepth.append(audio.bitdepth)
  duration.append(audio.duration)
  file_size.append(str(audio.filesize/1000000))
  sample_rate.append(audio.samplerate)
  speaker_name.append("speaker_"+a+".wav")


  sound = AudioSegment.from_wav("speaker_"+a+".wav")
  os.makedirs("/home/ubuntu/vlippr/youtube/"+lang+"/"+label+"/speaker_"+a)
  audio_chunks = split_on_silence(sound, min_silence_len=400, silence_thresh=-35 )
  #loop is used to iterate over the output list
  n=0
  dura=0
  for i, chunk in enumerate(audio_chunks):
    try:
      n=str(n)
      output_file = "/home/ubuntu/vlippr/youtube/"+lang+"/"+label+"/speaker_"+a+"/chunk{0}.wav".format(i)
      print("Exporting file", output_file)
      chunk.export(output_file, format="wav")
      audio = TinyTag.get(output_file)
      #duration.append(audio.duration)
      new=audio.duration
      dura=dura+new
      model = pretrained.dns64().cuda()
      wav, sr = torchaudio.load(output_file)
      wav = convert_audio(wav.cuda() ,sr, model.sample_rate, model.chin)
      with torch.no_grad():
          denoised = model(wav[None])[0]
      meta=disp.Audio(denoised.data.cpu().numpy(), rate=model.sample_rate)
      with open(output_file , "wb") as f:
        f.write(meta.data)
        n=int(n)
        n=n+1
      audio = TinyTag.get(output_file)
      new=audio.duration
      if new>20 or new<0.75:
        os.remove(output_file)
    except:
      print("Not found")
      pass



  duration_after_chunks.append(str(dura))
  os.remove("speaker_"+a+".wav")


  a=int(a)
  lin+=1
  a+=1
df=pd.DataFrame(list(zip(channel_name ,channel_URL, video_link_1 , video_link_2, video_link_3 , video_link_4 ,video_link_5 , speaker_name ,speaker_language , speaker_gender , sample_rate , Bitrate , audio_offset, bitdepth , duration ,duration_after_chunks, file_size )),columns=["Channel Name" ,"Channel URL", "Video Link 1" , "Video Link 2", "Video Link 3" , "Video Link 4" ,"Video Link 5" , "Speaker Name" , "Speaker Language" , "Speaker Gender" , "Sample Rate" , "Bitrate" , "Audio Offset", "Bitdepth" , "Duration in Seconds" ,"Duration After Chunks in Seconds","File Size in Mb"])
df.to_csv("data.csv")
df['Duration After Chunks in Seconds'] = df['Duration After Chunks in Seconds'].astype(float)
Total = sum(df['Duration After Chunks in Seconds'])/3600
Total=round(Total, 2)
a=str(a)
filee = open("myfile.txt","w")
filee.write("Total no. of speakers = "+a+" \nTotal speakers data scrapped = {} hours. ".format(Total))
filee.close()

